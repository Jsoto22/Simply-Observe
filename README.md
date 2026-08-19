# Simply Observe

A lightweight, dependency-free reactive primitives library for TypeScript/JavaScript — `Observer`, `Subject`, and a set of combinators for working with streams of values over time.

Inspired by RxJS, built from scratch with a smaller surface area and no external dependencies.

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

Static methods on `Observer` for combining multiple sources. Each accepts an array of `Observer`s and, where sources have different value types, infers a tuple so per-position types are preserved (`Observer.all([numberObs, stringObs])` returns `Observer<[number, string]>`, not `Observer<number | string>`).

| Method | Behavior |
|---|---|
| `Observer.all(observers)` | Waits for every source to complete, then emits one array of their final values. |
| `Observer.partial(observers)` | Emits a snapshot array after each individual source completes. |
| `Observer.latest(observers)` | Emits an updated array every time any source emits (`combineLatest`-style, may include `undefined` for sources that haven't emitted yet). |
| `Observer.zip(observers)` | Pairs values positionally — emits once every source has produced a value at that index. |
| `Observer.sequent(observers)` | Subscribes to sources one at a time, in order, emitting each one's value as it completes before moving to the next. |
| `Observer.race(observers)` | Mirrors whichever source emits or errors first; all others are unsubscribed. |
| `Observer.flat(observers)` | Merges all sources into a single stream, emitting values as they arrive from any source. |

```ts
import { Observer, of } from 'simply-observe';

Observer.zip([of(1, 2, 3), of('a', 'b', 'c')]).subscribe((pair) => {
  console.log(pair); // [1, 'a'], then [2, 'b'], then [3, 'c']
});
```

## `of(...)`

Constructs an `Observer` that synchronously emits the given values, in order, then completes.

```ts
import { of } from 'simply-observe';

of(1, 2, 3).subscribe((v) => console.log(v));
```

## Unsubscribing

`subscribe()` returns a `Subscription`. Call `.unsubscribe()` to stop receiving values and run any teardown logic.

```ts
const sub = observer.subscribe((v) => console.log(v));
sub.unsubscribe();
```

Subscriptions can be linked together — `subscription.add(other)` ties `other`'s lifecycle to `subscription`'s, so unsubscribing the parent tears down every linked child.

## License

MIT
