import ObserverContext from "./context";
import { CatchUnknown, CompleteHandler, NextHandler, Observer, OperatorFunction } from "./observer";
import Subscription from "./subscription";

export function of(value?: undefined): Observer<undefined>
export function of(value: null): Observer<null>
export function of<T>(...args: CatchUnknown<T>[]): Observer<T>
export function of<T>(...args: CatchUnknown<T>[]) {
    return new Observer<T>((next, _, complete) => {
        for (let item of args) {
            next(item)
        }
        complete()
    })
}

export function timeout<T>(value: CatchUnknown<T>, delay: number | undefined = 0) {
    return new Observer<T>((next, _, complete) => {
        const t = setTimeout(() => { next(value); complete(); }, delay);
        return () => clearTimeout(t);
    });
}

export function interval(delay: number | undefined = 0) {
    return new Observer<number>((next) => {
        let count = 0;
        const t = setInterval(() => next(count++), delay);
        return () => clearInterval(t);
    });
}

export function map<T, R>(fn: (value: T) => CatchUnknown<R>): OperatorFunction<T, R> {
    return (source: Observer<T>) => new Observer<R>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => next(fn(val)),
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function filter<T>(predicate: (value: T) => boolean): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => { if (predicate(val)) next(val as CatchUnknown<T>); },
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function tap<T>(fn: (value: T) => void): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => { fn(val); next(val as CatchUnknown<T>); },
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function catchError<T>(handler: (err: any) => Observer<T>): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        let inner: Subscription<T> | null = null;
        const outerSub = source.subscribe(
            next as NextHandler<T>,
            (err: any) => {
                const fallback = handler(err);
                inner = fallback.subscribe(next as NextHandler<T>, error, complete as CompleteHandler<T>);
            },
            complete as CompleteHandler<T>
        );

        return () => {
            outerSub.unsubscribe();
            inner?.unsubscribe();
        };
    });
}

export function retry<T>(count: number): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const onError = (err: any) => {
            if (count < 0) return;
            if (count === 0) error(err);
            count--;
            inner = source.subscribe(next as NextHandler<T>, onError, complete as CompleteHandler<T>);
        }
        let inner: Subscription<T> | null = null;
        const outerSub = source.subscribe(
            next as NextHandler<T>,
            onError,
            complete as CompleteHandler<T>
        );

        return () => {
            outerSub.unsubscribe();
            inner?.unsubscribe();
        };
    });
}

export function take<T>(count: number): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => {
                if (count < 0) return;
                if (count === 0) complete();
                count--;
                next(val as CatchUnknown<T>);
            },
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function takeUntil<T>(notifier: Observer<T>): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const notification = notifier.subscribe(() => complete())
        const sub = source.subscribe(
            (val) => next(val as CatchUnknown<T>),
            error,
            complete as CompleteHandler<T>
        );
        return () => {
            sub.unsubscribe();
            notification.unsubscribe();
        };
    });
}

export function skip<T>(count: number): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => {
                if (count > 0) {
                    count--;
                    return
                };
                next(val as CatchUnknown<T>);
            },
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function debounce<T>(value: number): OperatorFunction<T, T> {
    let w: any;
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => {
                if (w) clearTimeout(w);

                w = setTimeout(() => {
                    next(val as CatchUnknown<T>);
                }, value);
            },
            (err) => {
                clearTimeout(w);
                error(err)
            },
            () => {
                clearTimeout(w);
                complete()
            }
        );
        return () => { clearTimeout(w); sub.unsubscribe(); }
    });
}

export const isAllCompleted = (completed: boolean[]) => {
    return !completed.includes(false)
}
