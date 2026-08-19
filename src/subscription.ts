import { ObserverRef } from "./types"

export interface Subscription<T> {
    ref: ObserverRef
    closed: boolean
    parents: Subscription<unknown>[] | null
    unsubscribe(): void
    add<U>(subscription: Subscription<U>): void
    remove<U>(subscription: Subscription<U>): void
}

export interface SubscriptionConstructor {
    new <T>(_unsubscribe?: UnsubscribeHandler): Subscription<T>
}

export type UnsubscribeHandler = () => boolean

export const Subscription: SubscriptionConstructor = class Subscription<T> implements Subscription<T> {

    private _ref = crypto.randomUUID()
    private _linked: Subscription<unknown>[] = []
    private _parents: Subscription<unknown>[] | null = null
    private _closed = false

    constructor(private _unsubscribe: UnsubscribeHandler = () => true) { }

    public get ref() {
        return this._ref
    }

    public get closed() {
        return this._closed
    }

    public get parents() {
        return this._parents
    }

    public unsubscribe = () => {
        let error;

        if (!this._closed) {
            this._closed = true;

            const parents = this._parents;
            this._parents = null;
            if (parents) {
                for (let parent of parents) {
                    parent.remove(this);
                }
            }

            try {
                if (!this._unsubscribe()) throw Error('Error occurred during unsubscription');
            } catch (err: any) {
                error = err;
            }

            const linked = this._linked;
            this._linked = [];

            for (let subscription of linked) {
                subscription.unsubscribe();
            }

        }

        if (error) throw error;
    }

    public add = <U>(subscription: Subscription<U>) => {
        if (subscription !== this) {
            if (this._closed) subscription.unsubscribe();
            else {
                if (subscription.closed || subscription.hasParent(this)) return;
                subscription.linkParent(this);
                this._linked.push(subscription);
            }
        }
    }

    public remove = <U>(subscription: Subscription<U>) => {
        if (subscription !== this) {
            const index = this._linked.indexOf(subscription);
            0 <= index && this._linked.splice(index, 1);
            if (subscription.hasParent(this)) subscription.removeParent(this);
        }
    }

    private linkParent = <U>(subscription: Subscription<U>) => {
        if (!this._parents) this._parents = [];
        if (this.hasParent(subscription)) return;
        this._parents.push(subscription)
    }

    private removeParent = <U>(subscription: Subscription<U>) => {
        if (!Array.isArray(this._parents)) return;
        const index = this._parents.indexOf(subscription);
        0 <= index && this._parents.splice(index, 1);
        if (this._parents.length === 0) this._parents = null;
    }

    private hasParent = <U>(subscription: Subscription<U>) => {
        return this._parents?.includes(subscription)
    }

}

export default Subscription