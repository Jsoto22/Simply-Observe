import Subscription from "./subscription";
import { ObserverRef } from "./types";

export function isAllCompleted(subs: Map<ObserverRef, Subscription<unknown>>) {
    let isComplete = true;
    subs.forEach((v) => {
        if (!v.completed) isComplete = false;
    })
    return isComplete;
}

export function isFullSet(stream: Map<ObserverRef, any>) {
    let isFullSet = true;
    stream.forEach((val) => {
        if (!val) isFullSet = false;
    })
    return isFullSet;
}

export function unsubscribeAll(subs: Map<ObserverRef, Subscription<unknown>>) {
    subs.forEach((val) => {
        val.unsubscribe()
    })
}

export function closeAll(subs: Map<ObserverRef, Subscription<unknown>>) {
    subs.forEach((val) => {
        val.close()
    })
}

export function mapToArray(stream: Map<ObserverRef, any>){
    return Array.from(stream, (entry) => { return entry[1] })
}