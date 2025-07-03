/*
Package fsets provides a function set for executing a chain of function calls that pass a state object between them.
This is the mutant child of my statemachine package without fancy routing. It provides automatic retries and the idea
is to reduce testing chains like with the statemachine, but with linear calls.

Where this is helpful is avoiding doing testing of the entire call chain from a particular section of the chain down.
With this, you can test each function independently without having to mock out the next call. This also will let you still
access helper methods and fields of a struct so you can still use methods as part of the function set.

This also contains methods to run the function set in parallel or concurrently. This is useful for using the function set
in a way that allows for parallel execution of the function calls. It leverages promises to allow for getting the results back.

This is designed with a state object that is passed through the call chain that is stack allocated (though your data might not be).
This can help keep your allocations down.

Use is easy for regular functions:

	fset := fsets.Fset[MyDataType]{}
	fset.Adds(
		fsets.C[MyDataType]{F: myFunction1, B: myBackoff},
		fsets.C[MyDataType]{F: myFunction2},
	)

	so := fsets.StateObject[MyDataType]{Data: myData}
	so = fset.Run(ctx, so)
	if so.Err() != nil {
		log.Error("Error running function set", "error", err)
	}

Use with methods is also easy:

	fset := fsets.Fset[MyDataType]{}
	fset.Adds(
		fsets.C[MyDataType]{F: myStruct.myMethod1, B: myBackoff},
		fsets.C[MyDataType]{F: myStruct.myMethod2},
	)

	so := fsets.StateObject[MyDataType]{Data: myData}
	so = fset.Run(ctx, so)

You can also do this within a method of a struct:

	func (m *MyStruct) MyMethod(ctx context.Context, myData MyDataType) error {
		// Though it is usually better unless you need to do the setup dynamically to just
		// do this in the constructor to avoid the overhead of creating a new Fset every time.
		fset := fsets.Fset[MyDataType]{}
		fset.Adds(
			fsets.C[MyDataType]{F: m.myMethod1, B: myBackoff},
			fsets.C[MyDataType]{F: m.myMethod2},
		)

		return fset.Run(ctx, fsets.StateObject[MyDataType]{Data: myData}).Err()
	}

You will also notice that functions can have exponential backoff retries. You can set some to have this, others not and use
different backoffs for different functions. We also return the state object so that you can get return data that was stack
allocated and not heap allocated. This is useful for performance and memory usage.

In addition to the serial execution through Run() calls, you can also run the function set in parallel or concurrently. This is
useful when you want to have a some type of bound on the number of executions and reuse goroutines.
The provided methods allows you to feed in a channel of promises that will be executed in parallel or concurrently with the
function set. Parallel and concurrent execution is simply a function of the number of concurrent goroutines that are running the
function set. In parallel, there is a fixed number for execution, like say 5 instances running. In concurrent, there might be 5
instances running with another len(fset.Len()) instances running. This mimics the behavior of channel based concurrency execution.

You can use promises to a provide a bounded RPC call chain:

	type RPCServer struct {
		fset fsets.Fset[RPCRequest]
		in  fsets.PromiseQueue[RPCRequest, RPCResponse]
	}

	func New(ctx context.Context) *RPCServer {
		// Create a new Fset for the RPC server.
		fset := fsets.Fset[RPCRequest]{}

		r := &RPCServer{}
		// Add the functions to the Fset. These can be methods or functions.
		fset.Adds(
			fsets.C[RPCRequest]{F: r.handleAuth, B: myBackoff},
			fsets.C[RPCRequest]{F: r.handleData},
			fsets.C[RPCRequest]{F: r.handleResponse},
		)

		in, _ := fset.Concurrent(ctx, -1)

		r.fset = fset
		r.in = in
		return r
	}

	func (s *RPCServer) Serve(ctx context.Context, req RPCRequest) (RPCResponse, error) {
		// Create a new promise for the request.
		p := s.fset.Promise(req)

		if err := s.in.Send(ctx, p); err != nil {
			return RPCResponse{}, err // Context cancelled or other error.
		}

		// Wait for the response.
		resp, err := p.Get(ctx)
		if err != nil { // Only happens if Context is cancelled.
			return RPCResponse{}, err
		}

		return resp.V, resp.Err()
	}

It is similar if you want to use parallel execution instead using .Parallel() instead of .Concurrent().

You can also just use the Fset.Run(), which will be bounded only to the number of allowed RPC calls:

	func (s *RPCServer) Serve(ctx context.Context, req RPCRequest) (RPCResponse, error) {
		// Create a new StateObject for the request.
		so := fsets.StateObject[RPCRequest]{Data: req}

		// Run the Fset with the StateObject.
		resp := s.fset.Run(ctx, so)

		if resp.Err() != nil {
			return RPCResponse{}, resp.Err()
		}

		return resp.Data, nil
	}

You can also do an ordered pipeline by using the WithPipeline option when calling Concurrent or Parallel:

	fset := fsets.Fset[Data]{}

	// Add the functions to the Fset. These can be methods or functions.
	fset.Adds(
		fsets.C[Data]{F: handleAuth, B: myBackoff},
		fsets.C[Data]{F: handleData},
		fsets.C[Data]{F: handleResponse},
	)

	// Create a channel to get the results on.
	out := make(chan promises.Promise[StateObject[T], StateObject[T]], 1)
	in, _ := fset.Concurrent(ctx, -1, WithPipeline[Data](out))

	// Send data to the input channel.
	go func() {
		defer close(in) // Close the input channel when done.
		for _, data := range inputData{
			p := fset.Promise(data)
			in.Send(ctx, p) // Send the promise to the input channel.
		}
	}()

	// Get data from the output channel.
	for promise := range out {
		// Process the promise results.
		resp, err := p.Get(ctx)
		if err != nil { // Only happens if Context is cancelled.
			log.Error("Error getting promise result", "error", err)
			continue
		}
		if resp.Err() != nil {
			log.Error("Error processing data", "error", resp.Err())
			continue
		}

		log.Info("Processed data: ", resp.V)
	}

There is a cost to using fsets vs standard function call chains. The runtime has to do the calling of the function calls, which slows
down the execution. In addition, I don't believe Go is smart enough to inline the function calls, so you lose that optimization as well.

This can be seen with the following benchmark where we make 3 functions calls:

BenchmarkCallChain-10           573722551                2.073 ns/op
BenchmarkFset-10                 8607430               141.8 ns/op

This shows that the overhead of using fsets adds about 46ns per call. This is not a problem for most use cases that do any kind
of system call (disk, network, ...). However, if its a real tight loop using cache lines and no syscalls, you might want to
use a standard function call chain that can be inlined and avoid the runtime overhead.

There is an experimental compiler (fsetcodegen/) which has its own README so you understand the limitations and how to use it.
You can install it and use go generate to generate the compiled version of the Fset. This will significantly reduce the
overhead of using fsets and allow you to use the same API as the Fset, but with a compiled version that is much faster. There are
examples of this in testing/compiler and testing/fakeRPC.
*/
package fsets

import (
	"runtime"

	"github.com/gostdlib/base/concurrency/sync"
	"github.com/gostdlib/base/context"
	"github.com/gostdlib/base/retry/exponential"
	"github.com/gostdlib/base/values/generics/promises"
)

// StateObject is an object passed through the call chain for a single call. It passed along a data object of type T
// that is passed down the call chain. When the object is returned at the end of a Run() call, calling .Err() will determine
// if an error occurred in the call chain. Any function the call chain can call .Stop() to stop the call chain without
// erroring. The Set*() calls are used by the fsets compiler and not for users.
type StateObject[T any] struct {
	// Data is any data related to this call.
	Data T

	// Ctx is the context for this call. If a context is not provided, it will default to context.Background().
	Ctx context.Context

	// err is an error that can be set by any function in the call chain. If this is set, the call chain will stop.
	err  error
	stop bool
}

// Err returns and error if one occurred in the call chain.
func (s *StateObject[T]) Err() error {
	return s.err
}

// Stop sets the StateObject to stop, which will stop the call chain without erroring.
func (s *StateObject[T]) Stop() {
	s.stop = true
}

// GetStop returns true if the call chain should stop. This is set by the Stop() method.
// Normally not used by the user as this is checked internally. But useful if you decide to decompose
// the Fset object at a later time.
func (s *StateObject[T]) GetStop() bool {
	return s.stop
}

// SetCtx sets the context for the StateObject. This is not used by users but is provided for
// the fsets compiler.
func (s *StateObject[T]) SetCtx(ctx context.Context) {
	s.Ctx = ctx
}

// SetErr sets an error on the StateObject. This will stop the call chain and return the error in Err().
// This should not be used in normal code as the functions can just return an error which does this automatically.
func (s *StateObject[T]) SetErr(err error) {
	s.err = err
}

// C represents a function call, represented by F . If a user wants F to be called with an exponential backoff mechanism,
// they can supply if via B. O represents options for the Backoff.Retry call, which can be used to customize the retry behavior.
type C[T any] struct {
	// F is the function to call.
	F func(so StateObject[T]) (StateObject[T], error)

	// B is retrier to use for F. This can be nil.
	B *exponential.Backoff
	// O are options for the Backoff.Retry call.
	O []exponential.RetryOption
}

// exec is responsible for executing the function F with the provided StateObject.
// If a Backoff is provided, it will retry the function call using the Backoff.Retry method.
func (c C[T]) exec(so StateObject[T]) (StateObject[T], error) {
	if c.B == nil {
		return c.F(so)
	}
	err := c.B.Retry(
		so.Ctx,
		func(context.Context, exponential.Record) error {
			var err error
			so, err = c.F(so)
			return err
		},
		c.O...,
	)
	return so, err
}

// Fset is a set of functions in the order they are added. If any return an error, the calls stop.
type Fset[T any] struct {
	set      []C[T]
	compiled CompiledFset[T]

	promiseDo    sync.Once
	promiseMaker *promises.Maker[StateObject[T], StateObject[T]]

	outCloseDo sync.Once
	ran        bool
}

// Adds adds calls to the Fset. Calling this after Run() has been called will panic.
func (f *Fset[T]) Adds(calls ...C[T]) *Fset[T] {
	if f.ran {
		panic("Fset.Adds called after Fset.Run")
	}
	f.set = append(f.set, calls...)
	return f
}

// CompiledFset is a compiled version of the Fset by the fsetcodegen tool.
type CompiledFset[T any] func(ctx context.Context, so StateObject[T]) StateObject[T]

func (f *Fset[T]) Compiled(c CompiledFset[T]) {
	f.compiled = c
}

// Len returns the number of calls in the Fset.
func (f *Fset[T]) Len() int {
	return len(f.set)
}

// Run runs the Fset calls. This is thread-safe as long as all calls are thread-safe.
func (f *Fset[T]) Run(ctx context.Context, so StateObject[T]) StateObject[T] {
	f.ran = true

	var err error
	if ctx == nil {
		ctx = context.Background()
	}
	so.Ctx = ctx

	if f.compiled != nil {
		// If we have a compiled version, use that.
		return f.compiled(ctx, so)
	}

	for _, call := range f.set {
		so, err = call.exec(so)
		if err != nil {
			so.err = err
			return so
		}
		if so.stop {
			return so
		}
	}
	return so
}

type parallelOption[T any] struct {
	out chan promises.Promise[StateObject[T], StateObject[T]]
}

// ParallelOption is an option that can be used to configure the parallel execution of the Fset.
type ParallelOption[T any] func(p parallelOption[T]) (parallelOption[T], error)

// WithPipeline provides a channel to send the results of the Fset execution.
func WithPipeline[T any](out chan promises.Promise[StateObject[T], StateObject[T]]) ParallelOption[T] {
	return func(p parallelOption[T]) (parallelOption[T], error) {
		p.out = out
		return p, nil
	}
}

// PromiseQueue is a channel that can be used to send promises to the Fset for parallel or concurrent execution.
type PromiseQueue[T any] struct {
	ch chan promises.Promise[StateObject[T], StateObject[T]]
}

// Send sends a promise to the PromiseQueue. This is a blocking call until the promise is sent or the context is done.
// The context is attached to the StateObject in the promise, so it can be used for cancelation.
func (pq PromiseQueue[T]) Send(ctx context.Context, p promises.Promise[StateObject[T], StateObject[T]]) error {
	p.In.Ctx = ctx // Ensure the context is set on the StateObject.
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case pq.ch <- p:
	}
	return nil
}

// Closes the queue which will shut down the goroutines that are processing the promises once current operations
// are done.
func (pq PromiseQueue[T]) Close() {
	close(pq.ch)
}

// Parallel sets up a channel to run the Fset in parallel. This will spawn n goroutines that will execute this
// in parallel and return a channel that you input promises to. Closing the channel will shut down the goroutines.
// n is the number of goroutines to run in parallel. If n < 1, it will use gomaxprocs to determine the number of goroutines to run.
// The context passed cannot be canceled. Control cancelation in the StateObject.Ctx, which works only if your functions support
// it. The returned sync.Group can be used to wait for all goroutines to finish after closing the channel, if that is desired.
// Use the Promise() method to create a new promise for the Fset to use with this channel.
func (f *Fset[T]) Parallel(ctx context.Context, n int, options ...ParallelOption[T]) (PromiseQueue[T], *sync.Group) {
	return f.parallel(ctx, n, options...)
}

// Concurrent sets up a channel to run the Fset concurrently with n numbers of parallel goroutines that are executing
// each C in the Fset concurrently. This means there are n * len(fset) goroutines running concurrently. Have 4 C's in the Fset, and
// n == 2 will result in up to 8 different function calls running concurrently. If n < 1, it will use gomaxprocs to determine the
// number of goroutines to run. Use the Promise() method to create a new promise for the Fset to use with this channel.
func (f *Fset[T]) Concurrent(ctx context.Context, n int, options ...ParallelOption[T]) (PromiseQueue[T], *sync.Group) {
	// In reality, using numParallel * numStages is the same as spawning X goroutines for stages connected with channels and
	// x parallel goroutines for a set of stages. This avoids a lot of that overhead.
	if n < 1 {
		n = runtime.GOMAXPROCS(-1)
	}
	n = n * len(f.set)
	return f.parallel(ctx, n, options...)
}

func (f *Fset[T]) parallel(ctx context.Context, n int, options ...ParallelOption[T]) (PromiseQueue[T], *sync.Group) {
	if len(f.set) == 0 {
		panic("Fset.Concurrent called with no functions in the set")
	}

	po := parallelOption[T]{}
	for _, opt := range options {
		var err error
		po, err = opt(po)
		if err != nil {
			panic("Fset.Concurrent called with invalid option: " + err.Error())
		}
	}

	g := context.Pool(ctx).Limited(n).Group()

	pq := PromiseQueue[T]{ch: make(chan promises.Promise[StateObject[T], StateObject[T]], 1)}
	ctxNoCancel := context.WithoutCancel(ctx)

	// This is not in the limited pool(p) because this is the controller for the parallel execution.
	context.Pool(ctxNoCancel).Submit(
		ctxNoCancel,
		func() {
			if po.out != nil {
				defer func() {
					f.outCloseDo.Do(
						func() {
							g.Wait(context.Background())
							close(po.out)
						},
					)
				}()
			}

			for promise := range pq.ch {
				so := promise.In
				g.Go(
					ctxNoCancel,
					func(ctx context.Context) error {
						// Run the Fset with the provided StateObject.
						result := f.Run(so.Ctx, so)
						promise.Set(ctxNoCancel, result, result.Err())
						if po.out != nil {
							po.out <- promise
						}
						return nil
					},
				)
			}
		},
	)

	return pq, &g
}

// Promise helps create a new promise for the Fset for use with the Parallel or Concurrent methods.
// This is easier and more efficient than creating a new promise manually.
func (f *Fset[T]) Promise(data T) promises.Promise[StateObject[T], StateObject[T]] {
	f.promiseDo.Do(
		func() {
			if f.promiseMaker == nil {
				f.promiseMaker = &promises.Maker[StateObject[T], StateObject[T]]{}
			}
		},
	)

	so := StateObject[T]{Data: data}
	return f.promiseMaker.New(context.Background(), so)
}
