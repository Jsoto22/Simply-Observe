import { Subscription } from "./subscription";
import { ObserverContext } from "./context";
import { ObserverRef, SubscriptionParameters, ObserverTaskFunction, NextHandler, ErrorHandler, CompleteHandler, CatchUnknown, extractInputTuple, OperatorFunction, PipeResult } from "./types";
import { isAllCompleted } from "./utils";

export interface Observer<T> {
    readonly ref: ObserverRef
    close(): void
    get closed(): boolean
    subscribe(...args: SubscriptionParameters<T>): Subscription<T>
}

export interface ObserverConstructor {
    new <T extends any = never, R extends CatchUnknown<T> = CatchUnknown<T>>(context: ObserverTaskFunction<R>): Observer<R>
    all<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T> //forkJoin
    partial<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T> //forkJoinStream?
    zip<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T> // zip
    latest<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T> // latest
    sequent<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T[number]> // concat
    race<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T[number]> // race
    flat<T extends readonly unknown[]>(observers: extractInputTuple<T>): Observer<T[number]> // merge
}

export class ObserverLike<T> {
    readonly ref = crypto.randomUUID()
    protected _context: ObserverContext<T> = new ObserverContext<T>();
    public get closed() {
        return !this._context.active
    }
}

export const Observer: ObserverConstructor = class Observer<T> extends ObserverLike<T> implements Observer<T> {

    /**
     * Returns an Observer that emits an array containing the latest emmited\
     * values of all inner Observers, in the order of the Observer array given,\
     * when the inner Observer completes. All inner Observer must emmit a complete\
     * message for the outer observer to emmit its value.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */
    static all<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T> {

        const task: ObserverTaskFunction<T> = (next, error, complete) => {
            if (observers.length === 0) {
                next([] as unknown as T);
                complete();
                return () => { };
            }

            const root = new Subscription();
            const stream: unknown[] = Array(observers.length);
            const completed: boolean[] = Array(observers.length).fill(false);

            observers.forEach((obs, i) => {
                const sub = obs.subscribe((val) => {
                    stream[i] = val;
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    completed[i] = true;
                    if (isAllCompleted(completed)) {
                        root.unsubscribe();
                        next([...stream] as unknown as T);
                        complete();
                    }
                });
                root.add(sub);
            });

            return () => root.unsubscribe();
        };

        return new Observer<T>(task);
    }

    /**
     * Returns an Observer that emits an array containing the\
     * emmited values of all inner Observers, as they are completed,\
     * in the order of the Observer array given. This streams all\
     * inner Observer's latest values as they are individualy completed.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */
    static partial<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T> {

        const task: ObserverTaskFunction<T> = (next, error, complete) => {
            if (observers.length === 0) {
                complete();
                return () => { };
            }

            const root = new Subscription();
            const stream: unknown[] = Array(observers.length);
            const completed: boolean[] = Array(observers.length).fill(false);

            observers.map((obs, i) => {
                const sub = obs.subscribe((val) => {
                    stream[i] = val;
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    completed[i] = true;
                    next([...stream] as unknown as T);
                    if (isAllCompleted(completed)) {
                        root.unsubscribe();
                        complete();
                    };
                });
                root.add(sub)
            })
            return () => root.unsubscribe();
        };

        return new Observer<T>(task);
    }

    /**
     * Returns an Observer that emits an array containing the\
     * emmited values of all inner Observers. This streams all\
     * inner Observer's latest values as they are individualy emmited.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */
    static latest<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T> {

        const task: ObserverTaskFunction<T> = (next, error, complete) => {
            if (observers.length === 0) {
                next([] as unknown as T);
                complete();
                return () => { };
            }

            const root = new Subscription();
            const stream: unknown[] = Array(observers.length);
            const completed: boolean[] = Array(observers.length).fill(false);

            observers.forEach((obs, i) => {
                const sub = obs.subscribe((val) => {
                    stream[i] = val;
                    next([...stream] as unknown as T);
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    completed[i] = true;
                    if (isAllCompleted(completed)) {
                        root.unsubscribe();
                        complete();
                    }
                });
                root.add(sub);
            });

            return () => root.unsubscribe();
        };

        return new Observer<T>(task);
    }

    /**
     * Returns an Observer that emits an array containing the\
     * emmited values of all inner Observers, in the order of the\
     * Observer array given. When the previous Observer completes,\
     * the next one is subscribed to, unsubscribing from the previous.\
     * Once all inner Observers are completed, all values are emmited\
     * by the outer Observer.
     * 
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

    static sequent<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T[number]> {

        const task: ObserverTaskFunction<T[number]> = (next, error, complete) => {
            if (observers.length === 0) {
                complete();
                return () => { };
            }

            const root = new Subscription();
            const queue = [...observers];

            const mountNext = () => {
                let value: T[number];
                let obs = queue.shift();

                if (!obs) {
                    root.unsubscribe();
                    complete();
                    return;
                }

                const sub = obs.subscribe((val) => {
                    value = val;
                    next(value);
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    mountNext();
                });
                root.add(sub);
            };

            mountNext();
            return () => root.unsubscribe();
        };

        return new Observer<T[number]>(task);
    }

    /**
     * Returns an Observer that mirrors the first inner Observer\
     * to emmit a value. All other Observers are ignored.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

    static race<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T[number]> {

        const task: ObserverTaskFunction<T[number]> = (next, error, complete) => {
            if (observers.length === 0) {
                complete();
                return () => { };
            }

            const root = new Subscription();
            let won = false;

            observers.forEach((obs) => {
                const sub = obs.subscribe((val) => {
                    if (won) return;
                    won = true;
                    root.unsubscribe();
                    next(val);
                    complete();
                }, (err) => {
                    if (won) return;
                    won = true;
                    root.unsubscribe();
                    error(err);
                }, () => {
                    // a source completing with no emission doesn't win
                });
                root.add(sub);
            });

            return () => root.unsubscribe();
        };

        return new Observer<T[number]>(task);
    }

    /**
     * Returns an Observer that merges the emmited values of the\
     * inner Observers as a single stream.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

    static flat<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T[number]> {

        const task: ObserverTaskFunction<T[number]> = (next, error, complete) => {
            if (observers.length === 0) {
                complete();
                return () => { };
            }

            const root = new Subscription();
            const completed: boolean[] = Array(observers.length).fill(false);

            observers.forEach((obs, i) => {
                const sub = obs.subscribe((val) => {
                    next(val);
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    completed[i] = true;
                    if (isAllCompleted(completed)) {
                        root.unsubscribe();
                        complete();
                    }
                });
                root.add(sub);
            });

            return () => root.unsubscribe();
        };

        return new Observer<T[number]>(task);
    }

    /**
     * Returns an Observer that merges the emmited values of the\
     * inner Observers at the same index. The outer Observer waits\
     * for a complete set before emmiting the values. If the inner\
     * Observers 
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */
    static zip<T extends readonly unknown[]>(
        observers: extractInputTuple<T>
    ): Observer<T> {

        const task: ObserverTaskFunction<T> = (next, error, complete) => {
            if (observers.length === 0) {
                next([] as unknown as T);
                complete();
                return () => { };
            }

            const root = new Subscription();
            const streams: unknown[][] = observers.map(() => []);
            const completed: boolean[] = Array(observers.length).fill(false);

            const checkReady = () => {
                const min = Math.min(...streams.map((s) => s.length));
                if (min > 0) {
                    next(streams.map((stream) => stream.shift()) as unknown as T);
                }
            };

            observers.forEach((obs, i) => {
                const sub = obs.subscribe((val) => {
                    streams[i].push(val);
                    checkReady();
                }, (err) => {
                    root.unsubscribe();
                    error(err);
                }, () => {
                    completed[i] = true;
                    if (isAllCompleted(completed)) {
                        root.unsubscribe();
                        complete();
                    }
                });
                root.add(sub);
            });

            return () => root.unsubscribe();
        };

        return new Observer<T>(task);
    }

    constructor(private task: ObserverTaskFunction<T>) {
        super();
    }

    public close() {
        if (!this._context.active) return;
        this._context.close();
    }

    public pipe<R extends OperatorFunction<any, any>[]>(...operators: R): Observer<PipeResult<T, R>> {
        let result: any = this;
        for (const op of operators) {
            result = op(result);
        }
        return result;
    }

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {
        if (!this._context.active) this._context = new ObserverContext();
        const teardown = this._context.add({ next, error, complete });
        if (!this._context.invoked) this._context.invoke(this.task);
        return new Subscription(teardown);
    }
}

export * from "./types"
export default { Observer }