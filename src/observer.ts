import { Subscribable, Subscription } from "./subscription";
import { ObserverContext } from "./context";
import { OperatorFunction, Pipeable, toPipe } from "./pipes";
import {
    ObserverTaskFunction,
    NextHandler,
    ErrorHandler,
    CompleteHandler,
} from "./types";

export interface Observer<T> extends Subscribable<T>, Pipeable<T> { }
export interface ObserverConstructor {
    new <T extends any = never>(context: ObserverTaskFunction<T>): Observer<T>
}

export class ObserverLike<T> {
    readonly ref = crypto.randomUUID()
    protected _context: ObserverContext<T> = new ObserverContext<T>();
    public get closed() {
        return !this._context.active
    }
}

export const Observer: ObserverConstructor = class Observer<T> extends ObserverLike<T> implements Observer<T> {

    constructor(private task: ObserverTaskFunction<T>) {
        super();
    }

    public close() {
        if (!this._context.active) return;
        this._context.close();
    }

    public subscribe(next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>) {
        if (!this._context.active) this._context = new ObserverContext();
        const teardown = this._context.add({ next, error, complete });
        if (!this._context.invoked) this._context.invoke(this.task);
        return new Subscription(teardown);
    }

    public pipe(...operators: OperatorFunction<any, any>[]) {
        return toPipe(operators)(this);
    }
}

export default { Observer }