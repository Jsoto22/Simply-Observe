import { Observer } from "./observer";
import Subscription from "./subscription";
import { CatchUnknown, extractInputTuple, ObserverTaskFunction } from "./types";

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

export function timeout<T>(value: T, delay: number | undefined = 0) {
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


/**
     * Returns an Observer that emits an array containing the latest emmited\
     * values of all inner Observers, in the order of the Observer array given,\
     * when the inner Observer completes. All inner Observer must emmit a complete\
     * message for the outer observer to emmit its value.
     * 
     * @param observers Observer array
     * @returns  An Observer that subscribes to all inner Observers
     */

export function all<T extends readonly unknown[]>(
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
export function partial<T extends readonly unknown[]>(
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
export function latest<T extends readonly unknown[]>(
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

export function sequent<T extends readonly unknown[]>(
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

export function race<T extends readonly unknown[]>(
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

export function flat<T extends readonly unknown[]>(
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
export function zip<T extends readonly unknown[]>(
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

const isAllCompleted = (completed: boolean[]) => {
    return !completed.includes(false)
}
