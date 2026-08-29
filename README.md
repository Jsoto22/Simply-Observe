# Simply Observe

A lightweight, dependency-free reactive primitives library for TypeScript/JavaScript. This includes `Observer` and `Subject` classes, with combinators and pipeable operators for working with streams of values over time.
 
Inspired by RxJS, built from scratch with a smaller surface area, and no external dependencies.
 
Originally built to share state between components in frameworks like React that have no built-in singleton or service pattern — a `Subject` created once and subscribed to from multiple components can sidestep complex prop drilling or managing Context entirely.
 
## Install
 
```bash
npm install simply-observe
```
 
## Quick start
 
### Observer
 
An `Observer` wraps a task function that receives `next`, `error`, and `complete` dispatchers. Subscribers are added lazily — the task only runs once something subscribes.
 
```ts
import { Observer } from 'simply-observe';
 
const observer = new Observer<number>((next, error, complete) => {
  next(1);
  next(2);
  complete();
 
  // optional teardown, called on unsubscribe/close
  return () => {
    console.log('cleaned up');
  };
});
 
observer.subscribe(
  (value) => console.log('next:', value),
  (err) => console.log('error:', err),
  () => console.log('complete')
);
```
 
### Subject
 
A `Subject` is both an Observer and a dispatcher — call `.next()`, `.error()`, or `.complete()` on it directly to push values to its subscribers.
 
```ts
import { Subject } from 'simply-observe';
 
const subject = new Subject<number>(0);
 
subject.subscribe((value) => console.log('got:', value));
 
subject.next(1);
subject.next(2);
subject.complete();
```
 
Late subscribers (subscribing after `error()`, `complete()`, or `close()` has already fired) immediately receive that terminal event rather than hanging silently.
 
## Combinators
 
Standalone functions for combining multiple sources. Each accepts an array of `Observer`s and, where sources have different value types, infers a tuple so per-position types are preserved (`all([numberObs, stringObs])` returns `Observer<[number, string]>`, not `Observer<number | string>`).
 
| Function | Behavior |
|---|---|
| `all(observers)` | Waits for every source to complete, then emits one array of their final values. |
| `partial(observers)` | Emits a snapshot array after each individual source completes. |
| `latest(observers)` | Emits an updated array every time any source emits (`combineLatest`-style, may include `undefined` for sources that haven't emitted yet). |
| `zip(observers)` | Pairs values positionally — emits once every source has produced a value at that index. |
| `sequent(observers)` | Subscribes to sources one at a time, in order, emitting each one's value as it completes before moving to the next. |
| `race(observers)` | Mirrors whichever source emits or errors first; all others are unsubscribed. |
| `flat(observers)` | Merges all sources into a single stream, emitting values as they arrive from any source. |
 
```ts
import { zip, of } from 'simply-observe';
 
zip([of(1, 2, 3), of('a', 'b', 'c')]).subscribe((pair) => {
  console.log(pair); // [1, 'a'], then [2, 'b'], then [3, 'c']
});
```

## Utils
 
### `of(...)`
 
Constructs an `Observer` that synchronously emits the given values, in order, then completes.
 
```ts
import { of } from 'simply-observe';
 
of(1, 2, 3).subscribe((v) => console.log(v));
```
 
###  `timeout` and `interval`
 
```ts
import { timeout, interval } from 'simply-observe';
 
// emits `value` once, after `delay` ms, then completes
timeout('done', 500).subscribe((v) => console.log(v));
 
// emits an increasing count every `delay` ms, never completes on its own
const sub = interval(1000).subscribe((count) => console.log(count));
sub.unsubscribe(); // stop it
```
 
## Pipeable operators
 
`Observer` has a `.pipe()` method for chaining transformations. Each operator is a standalone function that returns an `OperatorFunction`. A guide for custom operators will be coming soon.
 
```ts
import { of, map, filter } from 'simply-observe';
 
of(1, 2, 3, 4, 5)
  .pipe(
    filter((n) => n % 2 === 0),
    map((n) => n * 10)
  )
  .subscribe((v) => console.log(v)); // 20, 40
```
 
| Operator | Behavior |
|---|---|
| `map(fn)` | Transforms each emitted value. |
| `filter(predicate)` | Only emits values that pass the predicate. |
| `tap(fn)` | Runs a side effect for each value without altering it. |
| `take(count)` | Emits only the first `count` values, then completes. |
| `skip(count)` | Ignores the first `count` values, then emits the rest. |
| `debounce(delay)` | Waits for a pause of `delay` ms with no new emissions before emitting the latest value. |
| `takeUntil(notifier)` | Stops emitting (and completes) as soon as `notifier` emits. |
| `retry(count)` | On error, re-subscribes to the source up to `count` times before propagating the error. |
| `catchError(handler)` | On error, subscribes to the `Observer` returned by `handler(err)` instead of propagating the error. |
 
A thrown error inside `map`/`filter`/`tap`'s callback is caught automatically and routed to the stream's `error` channel so you don't need to wrap operator callbacks in your own try/catch.
 
## Unsubscribing
 
`subscribe()` returns a `Subscription`. Call `.unsubscribe()` to stop receiving values and run any teardown logic.
 
```ts
const sub = observer.subscribe((v) => console.log(v));
sub.unsubscribe();
```
 
Subscriptions can be linked together — `subscription.add(other)` ties `other`'s lifecycle to `subscription`'s, so unsubscribing the parent tears down every linked child.
 
## License
 
MIT