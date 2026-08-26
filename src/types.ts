import { Observer } from "./observer"
import Subscription from "./subscription";

export type Flat<T extends readonly unknown[]> = T extends [infer F, ...infer R] ? F extends readonly unknown[] ? [...F, ...Flat<R>] : [F, ...Flat<R>] : [];
export type Pop<T extends readonly unknown[]> = T extends [...infer R, infer _] ? R : never;

export type ObserverRef = ReturnType<Crypto["randomUUID"]>
export type ObserverTaskFunction<T> = (...args: ObserverTaskParameters<T>) => any
export type ObserverTaskParameters<T> = [update: UpdateDispatcher<T>, error: ErrorDispatcher, complete: CompleteDispatcher<T>]

export type UpdateDispatcher<T> = (next: T) => any
export type ErrorDispatcher = (err: any) => any
export type CompleteDispatcher<T> = (final?: T) => void

export type SubscriptionFunction<T> = (...args: SubscriptionParameters<T>) => Subscription<T>
export type SubscriptionParameters<T> = [next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T>]
export type SubscriptionHandlers<T> = { next: NextHandler<T>, error?: ErrorHandler, complete?: CompleteHandler<T> }
export type UnsubscribeHandler = () => boolean

export type NextHandler<T> = UpdateDispatcher<T>
export type ErrorHandler = ErrorDispatcher
export type CompleteHandler<T> = CompleteDispatcher<T>
export type CloseHandler = () => void

export type IsAny<T> = 0 extends 1 & T ? true : false;
export type CatchUnknown<T> = IsAny<T> extends true ? any : unknown extends T ? never : T;
export type extractUnknown<T> = T extends CatchUnknown<infer U> ? U : T

export type extractType<T> = T extends Observer<infer U>[] ? U : never
export type extractInputTuple<T> = { [K in keyof T]: Observer<T[K]> }

