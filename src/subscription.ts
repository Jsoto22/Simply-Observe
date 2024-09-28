import { CloseHandler, ObserverRef } from "./types"

export interface Subscription<T> {
    parent: ObserverRef
    ref: ObserverRef
    completed: boolean
    unsubscribe(): void
    close(): void
}

export interface SubscriptionConstructor {
    new <T>(_parent: ObserverRef, _unsubscribe: UnsubscribeHandler, _completed: () => boolean, _close: CloseHandler): Subscription<T>
}

export type UnsubscribeHandler = <T>(subscription: Subscription<T>) => boolean

export const Subscription: SubscriptionConstructor = class Subscription<T> implements Subscription<T> {

    private _ref = crypto.randomUUID()

    constructor(private _parent: ObserverRef, private _unsubscribe: UnsubscribeHandler, private _completed: () => boolean, private _close: CloseHandler) {

    }

    public get parent() {
        return this._parent
    }

    public get ref() {
        return this._ref
    }

    public get completed() {
        return this._completed()
    }

    public unsubscribe() {
        this._unsubscribe(this)
    }

    public close() {
        this._close()
    }

}

export default Subscription