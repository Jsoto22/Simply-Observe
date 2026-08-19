import { CatchUnknown, Observer } from "./observer";

export function of(value?: undefined): Observer<undefined>
export function of(value: null): Observer<null>
export function of<T>(...args: CatchUnknown<T>[]): Observer<T>
export function of<T>(...args: CatchUnknown<T>[]) {
    return new Observer<T>((next, _, complete) => {
        for (let item of args) {
            next(item)
        }
        complete()
    })
}


export const isAllCompleted = (completed: boolean[]) => {
    return !completed.includes(false)
}
