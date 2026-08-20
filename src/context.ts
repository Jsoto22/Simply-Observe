import { ObserverTaskFunction, SubscriptionHandlers } from "./types"

export interface ObserverContext<T> {
    add(subscriber: SubscriptionHandlers<T>): () => boolean
    invoke(task: ObserverTaskFunction<T>): void
    update(value?: T): void
    error(value?: unknown): void
    complete(final?: T): void
    close(): void
    active: boolean
    invoked: boolean
}

export interface ObserverContextConstructor {
    new <T>(): ObserverContext<T>
}

export const ObserverContext: ObserverContextConstructor = class ObserverContext<T> implements ObserverContext<T> {

    private _invoked = false
    private _closed = false;
    private _teardown?: Function;
    private _subscribers = new Set<SubscriptionHandlers<T>>();
    public get active() { return !this._closed }
    public get invoked() { return this._invoked }

    constructor() { }

    private finalize() {
        if (this._teardown && typeof this._teardown === 'function') {
            this._teardown();
        }
    }


    public add = (subscriber: SubscriptionHandlers<T>) => {
        this._subscribers.add(subscriber)
        return () => {
            this._subscribers.delete(subscriber)
            if (this._subscribers.size === 0) {
                this.close();
            }
            return true;
        }
    }

    public invoke = (task: ObserverTaskFunction<T>) => {
        this._invoked = true;
        this._teardown = task(this.update, this.error, this.complete)
    }

    public close = () => {
        if (!this.active) return;
        this._closed = true;
        this._subscribers.clear();
        this.finalize();
    }

    public update = (value?: T) => {
        if (!this.active) return;
        for (let { next } of this._subscribers.values()) {
            if (next || typeof next === 'function') {
                try {
                    next(value!);
                } catch (error) {
                    this.error(error)
                }
            }
        }
    }

    public error = (value?: unknown) => {
        if (!this.active) return;
        this._closed = true;                    // mark closed FIRST
        const subscribers = [...this._subscribers.values()];  // snapshot before clearing
        this._subscribers.clear();
        for (let { error } of subscribers) {
            if (error || typeof error === 'function') {
                error(value);
            }
        }
        this.finalize();
    }

    public complete = (final?: T) => {
        if (!this.active) return;
        this._closed = true;
        const subscribers = [...this._subscribers.values()];
        this._subscribers.clear();
        for (let { complete } of subscribers) {
            if (complete || typeof complete === 'function') {
                complete(final);
            }
        }
        this.finalize();
    }
}

export default ObserverContext