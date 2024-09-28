import { Subscription } from "./subscription";
import { ObserverContext } from "./context";
import { ObserverRef, SubscriptionParameters, ObserverTaskFunction, NextHandler, ErrorHandler, CompleteHandler, CatchUnknown, extractType } from "./types";
import { closeAll, isAllCompleted, isFullSet, mapToArray, unsubscribeAll } from "./utils";

export interface Observer<T> {
    readonly ref: ObserverRef
    get closed(): boolean
    get completed(): boolean
    subscribe(...args: SubscriptionParameters<T>): Subscription<T>
}

export interface ObserverConstructor {
    new <T extends any = never, R extends CatchUnknown<T> = CatchUnknown<T>>(context: ObserverTaskFunction<R>): Observer<R>
    all<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R[]> //forkJoin
    some<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R[]> //forkJoinStream?
    zip<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R[]> // zip
    latest<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R[]> // latest
    sequent<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R> // concat
    race<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R> // race
    flat<T extends Observer<unknown>[], R extends extractType<T>>(observers: Observer<R>[]): Observer<R> // merge
}

export class ObserverLike<T> {
    readonly ref = crypto.randomUUID()
    protected subscriptions = new Map<Subscription<T>, { next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T> }>();
    protected _closed: boolean = false;
    protected _completed: boolean = false;

    get closed() {
        return this._closed
    }

    get completed() {
        return this._completed
    }

    protected _isCompleted = () => {
        return this._completed
    }

    protected _update = (value?: T) => {
        if (this._closed) return;
        for (let { next } of this.subscriptions.values()) {
            if (next || typeof next === 'function') {
                next(value!);
            }
        }
    }

    protected _error = (value?: unknown) => {
        if (this._closed) return;
        for (let { error } of this.subscriptions.values()) {
            if (error || typeof error === 'function') {
                error(value);
                this._close();
            }
        }
    }

    protected _complete = (final?: T) => {
        if (this._closed) return;
        this._completed = true;
        for (let { complete } of this.subscriptions.values()) {
            if (complete || typeof complete === 'function') {
                complete(final);
            }
        }
    }

    protected _close = () => {
        if (this._closed) return;
        this._closed = true;
    }

    protected _unsubscribe = (subscription: Subscription<T>) => {
        if (subscription.parent !== this.ref) false;
        return this.subscriptions.delete(subscription);
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
    static all<T>(observers: Observer<T>[]): Observer<T[]> {
        const stream = new Map<ObserverRef, T|undefined>();
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T[]>((next, error, complete) => {
            observers.map((obs) => {
                stream.set(obs.ref, undefined)
                let sub = obs.subscribe((val: T) => {
                    stream.set(obs.ref, val);
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    subs.get(obs.ref)!.unsubscribe();
                    if (isAllCompleted(subs)) {
                        next(mapToArray(stream));
                        complete();
                    };
                });
                subs.set(obs.ref, sub);
            })
        })

        return this.override(observer, subs);
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
    static some<T>(observers: Observer<T>[]) {
        const stream = new Map<ObserverRef, T|undefined>();
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T[]>((next, error, complete) => {
            observers.map((obs) => {
                stream.set(obs.ref, undefined);
                let sub = obs.subscribe((val: T) => {
                    stream.set(obs.ref, val);
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    next(mapToArray(stream))
                    subs.get(obs.ref)!.unsubscribe();
                    if (isAllCompleted(subs)) complete();
                });
                subs.set(obs.ref, sub);
            });
        });

        return this.override(observer, subs);
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
    static latest<T>(observers: Observer<T>[]) {
        const stream = new Map<ObserverRef, T|undefined>();
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T[]>((next, error, complete) => {
            observers.map((obs) => {
                stream.set(obs.ref, undefined);
                let sub = obs.subscribe((val: T) => {
                    stream.set(obs.ref, val);
                    next(mapToArray(stream));
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    subs.get(obs.ref)!.unsubscribe();
                    if (isAllCompleted(subs)) complete();
                });
                subs.set(obs.ref, sub);
            });
        });

        return this.override(observer, subs);
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

    static sequent<T>(observers: Observer<T>[]) {
        const stream = new Map<ObserverRef, T>();
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T>((next, error, complete) => {
            let i = 0;
            const mountNext = () => {
                let obs = observers[i];
                let sub = obs.subscribe((val: T) => {
                    stream.set(obs.ref, val);
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    subs.get(obs.ref)!.unsubscribe();
                    next(stream.get(obs.ref)!);
                    i++;
                    if (i < observers.length) mountNext();
                    if (isAllCompleted(subs)) complete();
                });
                subs.set(obs.ref, sub);
            }

            mountNext();
        });

        return this.override(observer, subs);
    }

    /**
     * Returns an Observer that mirrors the first inner Observer\
     * to emmit a value. All other Observers are ignored.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

    static race<T>(observers: Observer<T>[]) {
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T>((next, error, complete) => {

            const setFocus = (ref: ObserverRef) => {
                focus = ref;
                subs.forEach((val, key) => {
                    if (key !== ref) val.unsubscribe();
                });
            }

            let focus: string;

            observers.map((obs) => {
                let sub = obs.subscribe((val: T) => {
                    if (!focus) setFocus(obs.ref);
                    if (focus !== obs.ref) return;
                    next(val);
                }, (err) => {
                    if (!focus) setFocus(obs.ref);
                    if (focus !== obs.ref) return;
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    if (!focus) setFocus(obs.ref);
                    if (focus !== obs.ref) return;
                    subs.get(obs.ref)!.unsubscribe();
                    complete();
                })
                subs.set(obs.ref, sub);
            })
        })

        return this.override(observer, subs);
    }

    /**
     * Returns an Observer that merges the emmited values of the\
     * inner Observers as a single stream.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

    static flat<T>(observers: Observer<T>[]) {
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T>((next, error, complete) => {

            observers.map((obs) => {
                let sub = obs.subscribe((val: T) => {
                    next(val);
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    subs.get(obs.ref)!.unsubscribe();
                    if (isAllCompleted(subs)) complete();
                })
                subs.set(obs.ref, sub);
            })
        })

        return this.override(observer, subs);
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

    static zip<T>(observers: Observer<T>[]) {
        const stream = new Map<ObserverRef, T|undefined>();
        const subs = new Map<ObserverRef, Subscription<T>>();
        const observer = new Observer<T[]>((next, error, complete) => {

            const updateSet = () => {
                next(mapToArray(stream));
                subs.forEach((_, key) => {
                    stream.set(key, undefined)
                });
            }

            observers.map((obs) => {
                stream.set(obs.ref, undefined);
                let sub = obs.subscribe((val: T) => {
                    if (isFullSet(stream)) updateSet();
                    stream.set(obs.ref, val);
                }, (err) => {
                    error(err);
                    unsubscribeAll(subs);
                }, () => {
                    subs.get(obs.ref)!.unsubscribe();
                    if (isAllCompleted(subs)) complete();
                })
                subs.set(obs.ref, sub);
            })
        })

        return this.override(observer, subs);
    }

    private static override<T>(observer: Observer<T>,subs: Map<ObserverRef, Subscription<T>>) {
        observer._unsubscribe = (subscription: Subscription<T>) => {
            if (subscription.parent !== observer.ref) false;
            unsubscribeAll(subs);
            return observer.subscriptions.delete(subscription);
        }

        observer._close = () => {
            if (observer._closed) return;
            observer._closed = true;
            closeAll(subs);
        }

        return observer
    }

    constructor(private task: ObserverTaskFunction<T>) {
        super();
        this.task = task;
    }

    private context: ObserverContext<T> = new ObserverContext<T>(this._update, this._error, this._complete);

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {

        if (!this.context.invoked) this.context.invoke(this.task);

        const subscription = new Subscription<T>(this.ref, this._unsubscribe, this._isCompleted, this._close);
        this.subscriptions.set(subscription, { next, error, complete });

        return subscription;
    }
}

export * from "./types"
export default { Observer, ObserverContext }