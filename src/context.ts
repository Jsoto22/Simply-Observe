import { ObserverRef, ObserverTaskFunction, ObserverTaskParameters, UpdateDispatcher, ErrorDispatcher, CompleteDispatcher } from "./types"

export interface ObserverContext<T> {
    invoked: boolean
    readonly completed: boolean
    readonly ref: ObserverRef
    invoke(task: ObserverTaskFunction<T>): void
}

export interface ObserverContextConstructor {
    new <T>(...args: ObserverTaskParameters<T>): ObserverContext<T>
}

export const ObserverContext: ObserverContextConstructor = class ObserverContext<T> implements ObserverContext<T> {

    private _invoked = false
    private _completed = false;
    private _task?: ObserverTaskFunction<T>;
    public get invoked() { return this._invoked }
    public readonly completed: boolean = this._completed;
    public readonly ref = crypto.randomUUID()

    constructor(private update: UpdateDispatcher<T>, private error: ErrorDispatcher, private complete: CompleteDispatcher<T>) { }

    public invoke(task: ObserverTaskFunction<T>) {
        this._invoked = true;
        this._task = task
        this._task(this.update, this.error, this.complete)
    }
}

export default ObserverContext