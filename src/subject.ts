
import { Observer, ObserverLike } from "./observer";
import { Subscription } from "./subscription";
import { CatchUnknown, CompleteHandler, ErrorHandler, NextHandler } from "./types";

export interface Subject<T> extends Observer<T>{
    next(value: T):void
    error(value: any):void
    complete():void
    close():void
}

export interface SubjectConstructor {
    new <T extends any = never, R extends CatchUnknown<T> = CatchUnknown<T>>(initial: R): Subject<R>
}

export const Subject:SubjectConstructor = class Subject<T> extends ObserverLike<T> implements Subject<T> {
    
    constructor(initial: T) {
        super()
        this.value = initial
    }

    private value;

    public next(value: T){
        this.value = value;
        this._update(value)
    }
    
    public error(value: any){
        this._error(value)
    }
    
    public complete(){
        this._complete()
    }

    public close(){
        this._close()
    }

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {

        const subscription = new Subscription<T>(this.ref, this._unsubscribe, this._isCompleted, this._close)
        this.subscriptions.set(subscription, { next, error, complete })

        next(this.value);

        return subscription
    }
}

export default Subject
