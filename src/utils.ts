import Subscription from "./subscription";
import { ObserverRef } from "./types";

export function isAllCompleted(subs: Map<ObserverRef, Subscription<any>>) {
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

export function unsubscribeAll(subs: Map<ObserverRef, Subscription<any>>) {
    subs.forEach((val, key) => {
        val.unsubscribe()
        console.log('unsub from', key)
    })
}

export function closeAll(subs: Map<ObserverRef, Subscription<any>>) {
    subs.forEach((val, key) => {
        val.close()
        console.log('close from', key)
    })
}

export function mapToArray(stream: Map<ObserverRef, any>){
    return Array.from(stream, ([_, v]) => { return v })
}