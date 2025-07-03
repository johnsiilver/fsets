package fsets

import (
	"errors"
	"testing"
	"time"

	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/base/values/generics/promises"
)

var backoff *exponential.Backoff

func init() {
	var err error
	backoff, err = exponential.New(
		exponential.WithPolicy(
			exponential.Policy{
				InitialInterval: 100,
				MaxInterval:     1 * time.Second,
				Multiplier:      1.1,
				MaxAttempts:     10,
			},
		),
	)
	if err != nil {
		panic(err)
	}
}

type Data struct {
	count int
}

func TestExec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		c       C[Data]
		wantErr bool
	}{
		{
			name: "Run without backoff",
			c: C[Data]{
				F: func(so StateObject[Data]) (StateObject[Data], error) {
					return so, nil
				},
			},
		},
		{
			name: "Run with backoff",
			c: C[Data]{
				F: func(so StateObject[Data]) (StateObject[Data], error) {
					if so.Data.count < 5 {
						so.Data.count++
						return so, errors.New("fail")
					}
					return so, nil
				},
				B: backoff,
			},
		},
	}

	for _, test := range tests {
		so := StateObject[Data]{}
		so.Ctx = t.Context()
		so, err := test.c.exec(so)
		switch {
		case test.wantErr && err == nil:
			t.Errorf("TestExec(%s): got err == nil, want err != nil", test.name)
			continue
		case !test.wantErr && err != nil:
			t.Errorf("TestExec(%s): got err == %s, want err == nil", test.name, err)
			continue
		case err != nil:
			continue
		}
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	fStandard := func(so StateObject[Data]) (StateObject[Data], error) {
		so.Data.count++
		return so, nil
	}
	fStop := func(so StateObject[Data]) (StateObject[Data], error) {
		so.Data.count++
		so.Stop()
		return so, nil
	}
	fErr := func(so StateObject[Data]) (StateObject[Data], error) {
		so.Data.count++
		return so, errors.New("error occurred")
	}

	tests := []struct {
		name      string
		c         []C[Data]
		wantCount int
		wantErr   bool
	}{
		{
			name:      "Success",
			c:         []C[Data]{{F: fStandard}, {F: fStandard}, {F: fStandard}},
			wantCount: 3,
		},
		{
			name:      "Stop",
			c:         []C[Data]{{F: fStandard}, {F: fStop}, {F: fStandard}},
			wantCount: 2,
		},
		{
			name:      "Error",
			c:         []C[Data]{{F: fStandard}, {F: fErr}, {F: fStandard}},
			wantCount: 2,
			wantErr:   true,
		},
	}

	for _, test := range tests {
		fsets := Fset[Data]{}
		fsets.Adds(test.c...)
		so := StateObject[Data]{Data: Data{}}
		so = fsets.Run(t.Context(), so)

		switch {
		case test.wantErr && so.Err() == nil:
			t.Errorf("TestRun(%s): got err == nil, want err != nil", test.name)
			continue
		case !test.wantErr && so.Err() != nil:
			t.Errorf("TestRun(%s): got err == %s, want err == nil", test.name, so.Err())
			continue
		case so.Err() != nil:
		}

		if so.Data.count != test.wantCount {
			t.Errorf("TestRun(%s): got count == %d, want count == %d", test.name, so.Data.count, test.wantCount)
		}
	}
}

func TestParallel(t *testing.T) {
	t.Parallel()

	f := func(so StateObject[Data]) (StateObject[Data], error) {
		if so.Data.count < 0 {
			return so, errors.New("negative count not allowed")
		}
		so.Data.count++

		return so, nil
	}

	set := Fset[Data]{}
	set.Adds(
		C[Data]{F: f},
		C[Data]{F: f},
		C[Data]{F: f},
	)

	in, wait := set.Parallel(t.Context(), 10)

	promises := make([]promises.Promise[StateObject[Data], StateObject[Data]], 0, 5)
	for range 5 {
		p := set.Promise(Data{count: 0})
		in.Send(t.Context(), p)
		promises = append(promises, p)
	}
	// This one should error.
	p := set.Promise(Data{count: -1})
	in.Send(t.Context(), p)

	_ = wait.Wait(t.Context())

	errCount := 0
	for _, promise := range promises {
		resp, _ := promise.Get(t.Context())
		if resp.Err != nil {
			errCount++
			continue
		}
		if resp.V.Data.count != 3 {
			t.Errorf("TestParallel: got count == %d, want count == 3", resp.V.Data.count)
		}
	}
}

func TestWithPipeline(t *testing.T) {
	t.Parallel()

	f := func(so StateObject[Data]) (StateObject[Data], error) {
		if so.Data.count < 0 {
			return so, errors.New("negative count not allowed")
		}
		so.Data.count++

		return so, nil
	}

	set := Fset[Data]{}
	set.Adds(
		C[Data]{F: f},
		C[Data]{F: f},
		C[Data]{F: f},
	)

	out := make(chan promises.Promise[StateObject[Data], StateObject[Data]], 10)
	in, _ := set.Parallel(t.Context(), 10, WithPipeline(out))

	go func() {
		for range 5 {
			p := set.Promise(Data{count: 0})
			in.Send(t.Context(), p)
		}
		// This one should error.
		p := set.Promise(Data{count: -1})
		in.Send(t.Context(), p)
		in.Close()
	}()

	errCount := 0
	for promise := range out {
		resp, _ := promise.Get(t.Context())
		if resp.Err != nil {
			errCount++
			continue
		}
		if resp.V.Data.count != 3 {
			t.Errorf("TestWithPipeline: got count == %d, want count == 3", resp.V.Data.count)
		}
	}
}
