import { Observer } from "./observer"


export type ObserverRef = ReturnType<Crypto["randomUUID"]>
export type ObserverTaskFunction<T> = (...args: ObserverTaskParameters<T>) => any
export type ObserverTaskParameters<T> = [update: UpdateDispatcher<T>, error: ErrorDispatcher, complete: CompleteDispatcher<T>]

export type UpdateDispatcher<T> = (next: T) => any
export type ErrorDispatcher = (err: any) => any
export type CompleteDispatcher<T> = (final?: T) => void

export type SubscriptionFunction<T> = (...args: SubscriptionParameters<T>) => any
export type SubscriptionParameters<T> = [next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>]
export type SubscriptionHandlers<T> = { next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T> }

export type NextHandler<T> = UpdateDispatcher<T>
export type ErrorHandler = {} & ErrorDispatcher
export type CompleteHandler<T> = {} & CompleteDispatcher<T>
export type CloseHandler = () => void

export type IsAny<T> = 0 extends 1 & T ? true : false;
export type CatchUnknown<T> = IsAny<T> extends true ? any : unknown extends T ? never : T;
export type extractType<T> = T extends Observer<infer U>[] ? U : never
export type extractInputTuple<T> = { [K in keyof T]: Observer<T[K]> }

export type OperatorFunction<T, R> = (source: Observer<T>) => Observer<R>;
export type PipeResult<T, U extends OperatorFunction<any, any>[]> =
    U extends [infer First, ...infer Rest]
    ? First extends OperatorFunction<T, infer R>
    ? Rest extends OperatorFunction<any, any>[]
    ? PipeResult<R, Rest>
    : R
    : never
    : T;




