
import { Observer, ObserverLike } from "./observer";
import { Subscribable, Subscription } from "./subscription";
import { CompleteHandler, ErrorHandler, NextHandler } from "./types";

type gaurdNever<T> = false extends isNever<T> ? [value: T] : [];
type isNever<T> = T extends never ? true : false;

export interface Subject<T> extends Subscribable<T> {
    close(): void
    next(...values: gaurdNever<T>): void;
    error(value?: any): void
    complete(final?: T): void
    asObservable(): Observer<T>
}

export interface SubjectConstructor {
    new <T extends any = never>(initial?: T): Subject<T>
}

export const Subject: SubjectConstructor = class Subject<T> extends ObserverLike<T> implements Subject<T> {

    private value?: T;
    private terminal?: { type: 'error'; value: any } | { type: 'complete'; value?: T };

    constructor(initial?: T) {
        super()
        this.value = initial
    }
    // public next(): void
    // public next(value: T): void
    public next(...values: gaurdNever<T>) {
        let value;
        if (values.length === 0) this._context.update();
        if (Array.isArray(values)) value = values[0];
        if (!this._context.active) return;
        this.value = value;
        this._context.update(value);
    }

    public error(value?: any) {
        if (!this._context.active) return;
        this.terminal = { type: 'error', value };
        this._context.error(value);
    }

    public complete(final?: T) {
        if (!this._context.active) return;
        this.terminal = { type: 'complete', value: final };
        this._context.complete(final);
    }

    public close() {
        if (!this._context.active) return;
        this.terminal = { type: "complete", value: void 0 };
        this._context.close();
    }

    public asObservable(): Observer<T> {
        return new Observer<T>((next, error, complete) => {
            const sub = this.subscribe(next, error, complete);
            return () => sub.unsubscribe();
        });
    }

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {
        if (this.terminal) {
            if (this.terminal.type === 'error') error?.(this.terminal.value);
            else complete?.(this.terminal.value);
            return new Subscription<T>(() => true);
        }

        const sub = new Subscription<T>(this._context.add({ next, error, complete }));
        if (this.value !== undefined) next(this.value);
        return sub;
    }
}


export default Subject
