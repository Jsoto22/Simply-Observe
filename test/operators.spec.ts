import { Observer } from '../src/observer';
import { map, filter, tap, take, skip, debounce, takeUntil, retry, catchError } from '../src/pipes';
import { of } from '../src/utils';

describe('pipe', () => {
    it('threads a single operator', () => {
        const received: number[] = [];
        of(1, 2, 3)
            .pipe(map((n: number) => n * 2))
            .subscribe((v) => received.push(v));
        expect(received).toEqual([2, 4, 6]);
    });

    it('threads multiple operators left to right', () => {
        const received: number[] = [];
        of(1, 2, 3, 4, 5)
            .pipe(
                filter((n: number) => n % 2 === 0),
                map((n: number) => n * 10)
            )
            .subscribe((v) => received.push(v));
        expect(received).toEqual([20, 40]);
    });

    it('returns the same observer unchanged when called with no operators', () => {
        const received: number[] = [];
        of(1, 2).pipe().subscribe((v) => received.push(v));
        expect(received).toEqual([1, 2]);
    });
});

describe('map', () => {
    it('transforms each emitted value', () => {
        const received: string[] = [];
        of(1, 2, 3)
            .pipe(map((n: number) => `#${n}`))
            .subscribe((v) => received.push(v));
        expect(received).toEqual(['#1', '#2', '#3']);
    });

    it('routes a thrown projection error to the error handler instead of crashing', () => {
        const onError = jest.fn();
        const onNext = jest.fn();
        of(1, 2, 3)
            .pipe(
                map((n: number) => {
                    if (n === 2) throw 'bad value';
                    return n;
                })
            )
            .subscribe(onNext, onError);
        expect(onError).toHaveBeenCalledWith('bad value');
        expect(onNext).toHaveBeenCalledWith(1);
        expect(onNext).not.toHaveBeenCalledWith(3);
    });

    it('propagates complete()', () => {
        const onComplete = jest.fn();
        of(1).pipe(map((n: number) => n)).subscribe(() => { }, undefined, onComplete);
        expect(onComplete).toHaveBeenCalled();
    });
});

describe('filter', () => {
    it('only emits values passing the predicate', () => {
        const received: number[] = [];
        of(1, 2, 3, 4, 5)
            .pipe(filter((n: number) => n > 2))
            .subscribe((v) => received.push(v));
        expect(received).toEqual([3, 4, 5]);
    });

    it('emits nothing if no values pass', () => {
        const onNext = jest.fn();
        of(1, 2, 3)
            .pipe(filter((n: number) => n > 10))
            .subscribe(onNext);
        expect(onNext).not.toHaveBeenCalled();
    });

    it('routes a thrown predicate error to the error handler', () => {
        const onError = jest.fn();
        of(1, 2)
            .pipe(
                filter((n: number) => {
                    throw 'predicate exploded';
                })
            )
            .subscribe(() => { }, onError);
        expect(onError).toHaveBeenCalledWith('predicate exploded');
    });
});

describe('tap', () => {
    it('calls the side-effect function without altering emitted values', () => {
        const sideEffects: number[] = [];
        const received: number[] = [];
        of(1, 2, 3)
            .pipe(tap((n: number) => sideEffects.push(n)))
            .subscribe((v) => received.push(v));
        expect(sideEffects).toEqual([1, 2, 3]);
        expect(received).toEqual([1, 2, 3]);
    });
});

describe('take', () => {
    it('emits only the first n values', () => {
        const received: number[] = [];
        of(1, 2, 3, 4, 5)
            .pipe(take(3))
            .subscribe((v) => received.push(v));
        expect(received).toEqual([1, 2, 3]);
    });

    it('completes after emitting n values', () => {
        const onComplete = jest.fn();
        of(1, 2, 3, 4, 5)
            .pipe(take(2))
            .subscribe(() => { }, undefined, onComplete);
        expect(onComplete).toHaveBeenCalledTimes(1);
    });

    it('does not emit a value past the requested count even though the source keeps running', () => {
        const received: number[] = [];
        of(1, 2, 3, 4, 5)
            .pipe(take(3))
            .subscribe((v) => received.push(v));
        expect(received.length).toBe(3);
        expect(received).not.toContain(4);
        expect(received).not.toContain(5);
    });

    it('completes immediately with no emissions when count is 0', () => {
        const onNext = jest.fn();
        const onComplete = jest.fn();
        of(1, 2, 3)
            .pipe(take(0))
            .subscribe(onNext, undefined, onComplete);
        expect(onNext).not.toHaveBeenCalled();
        expect(onComplete).toHaveBeenCalled();
    });
});

describe('skip', () => {
    it('skips the first n values', () => {
        const received: number[] = [];
        of(1, 2, 3, 4, 5)
            .pipe(skip(2))
            .subscribe((v) => received.push(v));
        expect(received).toEqual([3, 4, 5]);
    });

    it('emits everything when count is 0', () => {
        const received: number[] = [];
        of(1, 2, 3)
            .pipe(skip(0))
            .subscribe((v) => received.push(v));
        expect(received).toEqual([1, 2, 3]);
    });
});

describe('debounce', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    it('only emits the last value after the delay has passed with no new emissions', () => {
        const subject = new Observer<number>((next) => {
            (subject as any)._emit = next;
            return () => { };
        });
        const received: number[] = [];

        let emit: (v: number) => void = () => { };
        const source = new Observer<number>((next) => {
            emit = next;
            return () => { };
        });

        source.pipe(debounce(100)).subscribe((v) => received.push(v));

        emit(1);
        jest.advanceTimersByTime(50);
        emit(2);
        jest.advanceTimersByTime(50);
        emit(3);
        jest.advanceTimersByTime(100);

        expect(received).toEqual([3]);
    });

    it('does not use a shared timer across separate operator applications', () => {
        // regression: debounce's timer must be scoped per-subscription, not
        // hoisted to the operator factory, or two independent uses would
        // interfere with each other's timers
        let emitA: (v: number) => void = () => { };
        let emitB: (v: number) => void = () => { };
        const sourceA = new Observer<number>((next) => { emitA = next; return () => { }; });
        const sourceB = new Observer<number>((next) => { emitB = next; return () => { }; });

        const debounced = debounce<number>(100);
        const receivedA: number[] = [];
        const receivedB: number[] = [];

        sourceA.pipe(debounced).subscribe((v) => receivedA.push(v));
        sourceB.pipe(debounced).subscribe((v) => receivedB.push(v));

        emitA(1);
        jest.advanceTimersByTime(50);
        emitB(2);
        jest.advanceTimersByTime(100);

        // both should have resolved independently, not clobbered each other's timer
        expect(receivedA).toEqual([1]);
        expect(receivedB).toEqual([2]);
    });

    it('clears the pending timer on complete', () => {
        let emit: (v: number) => void = () => { };
        let complete: () => void = () => { };
        const source = new Observer<number>((next, _error, comp) => {
            emit = next;
            complete = comp;
            return () => { };
        });

        const received: number[] = [];
        source.pipe(debounce(100)).subscribe((v) => received.push(v));

        emit(1);
        complete();
        jest.advanceTimersByTime(200);

        expect(received).toEqual([]);
    });
});

describe('takeUntil', () => {
    it('stops emitting once the notifier fires', () => {
        let sourceEmit: (v: number) => void = () => { };
        const source = new Observer<number>((next) => {
            sourceEmit = next;
            return () => { };
        });

        let notify: () => void = () => { };
        const notifier = new Observer<void>((next) => {
            notify = () => next();
            return () => { };
        });

        const received: number[] = [];
        source.pipe(takeUntil(notifier)).subscribe((v) => received.push(v));

        sourceEmit(1);
        sourceEmit(2);
        notify();
        sourceEmit(3);

        expect(received).toEqual([1, 2]);
    });

    it('completes when the notifier fires', () => {
        let notify: () => void = () => { };
        const notifier = new Observer<void>((next) => {
            notify = () => next();
            return () => { };
        });
        const source = new Observer<number>(() => () => { });

        const onComplete = jest.fn();
        source.pipe(takeUntil(notifier)).subscribe(() => { }, undefined, onComplete);
        notify();

        expect(onComplete).toHaveBeenCalled();
    });
});

describe('retry', () => {
    it('re-subscribes to the source on error up to the given count', () => {
        let attempts = 0;
        const source = new Observer<string>((next, error, complete) => {
            attempts++;
            if (attempts < 3) {
                error('fail ' + attempts);
            } else {
                next('success');
                complete();
            }
        });

        const received: string[] = [];
        source.pipe(retry(5)).subscribe((v) => received.push(v));

        expect(attempts).toBe(3);
        expect(received).toEqual(['success']);
    });

    it('propagates the final error once the retry count is exhausted, with no extra attempts', () => {
        let attempts = 0;
        const source = new Observer<never>((_next, error) => {
            attempts++;
            error('fail ' + attempts);
        });

        const onError = jest.fn();
        source.pipe(retry(2)).subscribe(() => { }, onError);

        // regression: retry(2) should mean at most 2 retries -> 3 total attempts,
        // then exactly one error, with no wasted extra invocation afterward
        expect(attempts).toBe(3);
        expect(onError).toHaveBeenCalledTimes(1);
        expect(onError).toHaveBeenCalledWith('fail 3');
    });

    it('retry(0) makes exactly one attempt and errors immediately with no retry', () => {
        let attempts = 0;
        const source = new Observer<never>((_next, error) => {
            attempts++;
            error('fail');
        });

        const onError = jest.fn();
        source.pipe(retry(0)).subscribe(() => { }, onError);

        expect(attempts).toBe(1);
        expect(onError).toHaveBeenCalledTimes(1);
    });

    it('does not retry after a successful completion', () => {
        let attempts = 0;
        const source = new Observer<number>((next, _error, complete) => {
            attempts++;
            next(1);
            complete();
        });

        source.pipe(retry(5)).subscribe(() => { });
        expect(attempts).toBe(1);
    });
});

describe('catchError', () => {
    it('recovers with the fallback observer\'s values instead of propagating the error', () => {
        const failing = new Observer<number>((_next, error) => error('boom'));
        const received: number[] = [];
        failing.pipe(catchError(() => of(99))).subscribe((v) => received.push(v));
        expect(received).toEqual([99]);
    });

    it('completes after the fallback completes', () => {
        const failing = new Observer<number>((_next, error) => error('boom'));
        const onComplete = jest.fn();
        failing.pipe(catchError(() => of(1))).subscribe(() => { }, undefined, onComplete);
        expect(onComplete).toHaveBeenCalled();
    });

    it('passes the original error into the handler', () => {
        const handler = jest.fn(() => of(0));
        const failing = new Observer<number>((_next, error) => error('specific error'));
        failing.pipe(catchError(handler)).subscribe(() => { });
        expect(handler).toHaveBeenCalledWith('specific error');
    });

    it('does not call catchError\'s handler when the source succeeds', () => {
        const handler = jest.fn(() => of(0));
        of(1, 2).pipe(catchError(handler)).subscribe(() => { });
        expect(handler).not.toHaveBeenCalled();
    });

    it('propagates an error from the fallback observer itself', () => {
        const failing = new Observer<number>((_next, error) => error('original'));
        const fallback = new Observer<number>((_next, error) => error('fallback also failed'));
        const onError = jest.fn();
        failing.pipe(catchError(() => fallback)).subscribe(() => { }, onError);
        expect(onError).toHaveBeenCalledWith('fallback also failed');
    });
});