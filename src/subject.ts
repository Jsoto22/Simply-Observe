
import { Observer, ObserverLike } from "./observer";
import { Subscription } from "./subscription";
import { CatchUnknown, CompleteHandler, ErrorHandler, NextHandler } from "./types";

export interface Subject<T> extends Observer<T> {
    next(value: T): void
    error(value: any): void
    complete(): void
    close(): void
}

export interface SubjectConstructor {
    new <T extends any = never, R extends CatchUnknown<T> = CatchUnknown<T>>(initial: R): Subject<R>
}

export const Subject: SubjectConstructor = class Subject<T> extends ObserverLike<T> implements Subject<T> {

    private value: T;
    private terminal?: { type: 'error'; value: any } | { type: 'complete'; value?: T };

    constructor(initial: T) {
        super()
        this.value = initial
    }

    public next(value: T) {
        if (!this._context.active) return;
        this.value = value;
        this._context.update(value);
    }

    public error(value: any) {
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

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {
        if (this.terminal) {
            if (this.terminal.type === 'error') error?.(this.terminal.value);
            else complete?.(this.terminal.value);
            return new Subscription<T>(() => true);
        }
        return new Subscription<T>(this._context.add({ next, error, complete }));
    }
}

export default Subject
