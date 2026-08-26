import { Flat, Observer, Pop } from "./observer";
import Subscription from "./subscription";
import { CatchUnknown, CompleteHandler, NextHandler } from "./types";

export type OperatorFunction<T, R> = (source: Observer<T>) => Observer<R>;

export function map<T, R>(fn: (value: T) => R): OperatorFunction<T, R> {
    return (source: Observer<T>) => new Observer<R>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => next(fn(val)),
            error,
            complete as unknown as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function filter<T>(predicate: (value: T) => boolean): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => { if (predicate(val)) next(val); },
            error,
            complete as CompleteHandler<T>
        );
        return () => sub.unsubscribe();
    });
}

export function tap<T>(fn: (value: T) => void): OperatorFunction<T, T> {
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        const sub = source.subscribe(
            (val) => { fn(val); next(val); },
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
            if (count <= 0) {
                error(err);
                return;
            }
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
    return (source: Observer<T>) => new Observer<T>((next, error, complete) => {
        let timeout: any;
        const sub = source.subscribe(
            (val) => {
                if (timeout) clearTimeout(timeout);

                timeout = setTimeout(() => {
                    next(val as CatchUnknown<T>);
                }, value);
            },
            (err) => {
                clearTimeout(timeout);
                error(err)
            },
            () => {
                clearTimeout(timeout);
                complete()
            }
        );
        return () => { clearTimeout(timeout); sub.unsubscribe(); }
    });
}

export function toPipe<T, R>(operators: OperatorFunction<T, R>[]): OperatorFunction<T, R> {
    if (operators.length === 0) return (<T>(x: T): T => x) as OperatorFunction<T, R>;
    if (operators.length === 1) return operators[0] as OperatorFunction<T, R>;
    return (source: Observer<any>): Observer<R> => {
        return operators.reduce((a, f) => f(a), source as Observer<any>);
    }
}

type OpArgs<T, Args extends readonly unknown[]> = Flat<OpChain<Pop<[T, ...Args]>, Args>>
type OpChain<A extends readonly unknown[], B extends readonly unknown[]> = {
    [K in keyof A]: K extends keyof B ? [op: OperatorFunction<A[K], B[K]>] : never;
};

export interface Pipeable<T> {
    pipe(): Observer<T>;
    pipe<A>(...args: OpArgs<T, [A]>): Observer<A>;
    pipe<A, B>(...args: OpArgs<T, [A, B]>): Observer<B>;
    pipe<A, B, C>(...args: OpArgs<T, [A, B, C]>): Observer<C>;
    pipe<A, B, C, D>(...args: OpArgs<T, [A, B, C, D]>): Observer<D>;
    pipe<A, B, C, D, E>(...args: OpArgs<T, [A, B, C, D, E]>): Observer<E>;
    pipe<A, B, C, D, E, F>(...args: OpArgs<T, [A, B, C, D, E, F]>): Observer<F>;
    pipe<A, B, C, D, E, F, G>(...args: OpArgs<T, [A, B, C, D, E, F, G]>): Observer<G>;
    pipe<A, B, C, D, E, F, G, H>(...args: OpArgs<T, [A, B, C, D, E, F, G, H]>): Observer<H>;
    pipe<A, B, C, D, E, F, G, H, I>(...args: OpArgs<T, [A, B, C, D, E, F, G, H, I]>): Observer<I>;
    pipe<A, B, C, D, E, F, G, H, I>(
        op1: OperatorFunction<T, A>,
        op2: OperatorFunction<A, B>,
        op3: OperatorFunction<B, C>,
        op4: OperatorFunction<C, D>,
        op5: OperatorFunction<D, E>,
        op6: OperatorFunction<E, F>,
        op7: OperatorFunction<F, G>,
        op8: OperatorFunction<G, H>,
        op9: OperatorFunction<H, I>,
        ...operations: OperatorFunction<any, any>[]
    ): Observer<unknown>;
    pipe(...operators: OperatorFunction<any, any>[]): Observer<any>;
}