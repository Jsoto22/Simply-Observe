import { Subject } from '../src/subject';

describe('Subject', () => {
    it('delivers next() values to active subscribers', () => {
        const subject = new Subject<number>(0);
        const received: number[] = [];
        subject.subscribe((v) => received.push(v));
        subject.next(1);
        subject.next(2);
        expect(received).toEqual([0, 1, 2]);
    });

    it('delivers values to multiple independent subscribers', () => {
        const subject = new Subject<number>(0);
        const a: number[] = [];
        const b: number[] = [];
        subject.subscribe((v) => a.push(v));
        subject.subscribe((v) => b.push(v));
        subject.next(1);
        expect(a).toEqual([0, 1]);
        expect(b).toEqual([0, 1]);
    });

    describe('BehaviorSubject-style replay', () => {
        it('replays the initial value to a subscriber that joins before any next() call', () => {
            const subject = new Subject<number>(7);
            const received: number[] = [];
            subject.subscribe((v) => received.push(v));
            expect(received).toEqual([7]);
        });

        it('replays the most recently emitted value to a new subscriber', () => {
            const subject = new Subject<number>(0);
            subject.next(1);
            subject.next(2);

            const received: number[] = [];
            subject.subscribe((v) => received.push(v));

            expect(received).toEqual([2]);
        });

        it('does not replay a stale value to a late subscriber once terminal', () => {
            // terminal state must take priority over value-replay
            const subject = new Subject<number>(5);
            subject.next(10);
            subject.complete(99);

            const onNext = jest.fn();
            const onComplete = jest.fn();
            subject.subscribe(onNext, undefined, onComplete);

            expect(onNext).not.toHaveBeenCalled();
            expect(onComplete).toHaveBeenCalledWith(99);
        });
    });

    describe('terminal state', () => {
        it('replays complete() to a late subscriber instead of hanging silently', () => {
            const subject = new Subject<number>(0);
            subject.complete(42);

            const onNext = jest.fn();
            const onComplete = jest.fn();
            subject.subscribe(onNext, undefined, onComplete);

            expect(onComplete).toHaveBeenCalledWith(42);
            expect(onNext).not.toHaveBeenCalled();
        });

        it('replays error() to a late subscriber', () => {
            const subject = new Subject<number>(0);
            subject.error('boom');

            const onError = jest.fn();
            subject.subscribe(() => { }, onError);

            expect(onError).toHaveBeenCalledWith('boom');
        });

        it('ignores next() after complete()', () => {
            const subject = new Subject<number>();
            const received: number[] = [];
            subject.subscribe((v) => received.push(v));
            subject.complete();
            subject.next(99);
            expect(received).toEqual([]);
        });

        it('ignores next() after error()', () => {
            const subject = new Subject<number>();
            const received: number[] = [];
            subject.subscribe((v) => received.push(v));
            subject.error('boom');
            subject.next(99);
            expect(received).toEqual([]);
        });

        it('close() notifies late subscribers via complete() rather than hanging', () => {
            const subject = new Subject<number>(0);
            subject.close();

            const onNext = jest.fn();
            const onError = jest.fn();
            const onComplete = jest.fn();
            subject.subscribe(onNext, onError, onComplete);

            expect(onComplete).toHaveBeenCalled();
            expect(onNext).not.toHaveBeenCalled();
            expect(onError).not.toHaveBeenCalled();
        });

        it('close() does not overwrite an already-set terminal value from complete()', () => {
            const subject = new Subject<number>(0);
            subject.complete(42);
            subject.close();

            const onComplete = jest.fn();
            subject.subscribe(() => { }, undefined, onComplete);

            expect(onComplete).toHaveBeenCalledWith(42);
        });
    });

    describe('undefined as a value vs. no value emitted yet', () => {
        it('does NOT replay next(undefined) to a late subscriber -- known limitation', () => {
            // documents current behavior: value replay checks `!== undefined`,
            // so an intentional undefined emission is indistinguishable from
            // "nothing has been emitted yet" and will not be replayed.
            const subject = new Subject<number | undefined>(0);
            subject.next(undefined);

            const received: (number | undefined)[] = [];
            subject.subscribe((v) => received.push(v));

            expect(received).toEqual([]);
        });
    });

    describe('asObservable()', () => {
        it('returns an Observer that delivers subsequent next() calls', () => {
            const subject = new Subject<number>();
            const received: number[] = [];
            subject.asObservable().subscribe((v) => received.push(v));
            subject.next(1);
            subject.next(2);
            expect(received).toEqual([1, 2]);
        });

        it('replays the current value the same way subscribing directly does', () => {
            const subject = new Subject<number>(5);
            subject.next(10);

            const received: number[] = [];
            subject.asObservable().subscribe((v) => received.push(v));

            expect(received).toEqual([10]);
        });

        it('propagates complete() through to the observable view', () => {
            const subject = new Subject<number>(0);
            const onComplete = jest.fn();
            subject.asObservable().subscribe(() => { }, undefined, onComplete);
            subject.complete(42);
            expect(onComplete).toHaveBeenCalledWith(42);
        });

        it('propagates error() through to the observable view', () => {
            const subject = new Subject<number>(0);
            const onError = jest.fn();
            subject.asObservable().subscribe(() => { }, onError);
            subject.error('boom');
            expect(onError).toHaveBeenCalledWith('boom');
        });

        it('does not expose next/error/complete on the returned value at the type level', () => {
            const subject = new Subject<number>(0);
            const observable = subject.asObservable();
            // @ts-expect-error -- asObservable()'s return type should not include next()
            observable.next?.(1);
            expect(true).toBe(true); // presence of the ts-expect-error above is the real assertion
        });

        it('unsubscribing from the observable view does not affect other subscribers of the underlying subject', () => {
            const subject = new Subject<number>();
            const a: number[] = [];
            const b: number[] = [];
            const subA = subject.asObservable().subscribe((v) => a.push(v));
            subject.subscribe((v) => b.push(v));

            subA.unsubscribe();
            subject.next(1);

            expect(a).toEqual([]);
            expect(b).toEqual([1]);
        });
    });
});