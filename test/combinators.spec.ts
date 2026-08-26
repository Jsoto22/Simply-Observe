import { Observer } from '../src/observer';
import { all, flat, latest, of, partial, race, sequent, zip } from '../src/utils';

function asyncOf<T>(value: T, delay = 5): Observer<T> {
    return new Observer<T>((next, _error, complete) => {
        const t = setTimeout(() => {
            next(value);
            complete();
        }, delay);
        return () => clearTimeout(t);
    });
}

function asyncErrorAfter(err: any, delay = 5): Observer<never> {
    return new Observer<never>((_next, error) => {
        const t = setTimeout(() => error(err), delay);
        return () => clearTimeout(t);
    });
}

describe('all', () => {
    it('emits a single array of final values once every source completes', () => {
        const received: any[] = [];
        all([of(1, 2, 3), of('a', 'b')]).subscribe((v) => received.push(v));
        expect(received).toEqual([[3, 'b']]);
    });

    it('only emits once, not per-source', () => {
        const spy = jest.fn();
        all([of(1), of(2), of(3)]).subscribe(spy);
        expect(spy).toHaveBeenCalledTimes(1);
    });

    it('propagates an error from any source and stops', () => {
        const onError = jest.fn();
        const onNext = jest.fn();
        all([of(1), new Observer((_n, error) => error('boom'))]).subscribe(onNext, onError);
        expect(onError).toHaveBeenCalledWith('boom');
        expect(onNext).not.toHaveBeenCalled();
    });

    it('resolves correctly with async sources completing at different times', (done) => {
        all([asyncOf(1, 5), asyncOf(2, 15)]).subscribe((v) => {
            expect(v).toEqual([1, 2]);
            done();
        });
    });

    it('handles an empty array by emitting an empty array and completing', () => {
        const onNext = jest.fn();
        const onComplete = jest.fn();
        all([]).subscribe(onNext, undefined, onComplete);
        expect(onNext).toHaveBeenCalledWith([]);
        expect(onComplete).toHaveBeenCalled();
    });

    it('does not collide when the same observer instance appears more than once', () => {
        const shared = of(1, 2, 3);
        const received: any[] = [];
        all([shared, shared]).subscribe((v) => received.push(v));
        expect(received).toEqual([[3, 3]]);
    });
});

describe('partial', () => {
    it('emits a snapshot after each individual source completes', () => {
        const received: any[] = [];
        partial([of(1), of(2)]).subscribe((v) => received.push([...v]));
        expect(received.length).toBe(2);
    });

    it('handles an empty array by completing with no emissions', () => {
        const onNext = jest.fn();
        const onComplete = jest.fn();
        partial([]).subscribe(onNext, undefined, onComplete);
        expect(onNext).not.toHaveBeenCalled();
        expect(onComplete).toHaveBeenCalled();
    });
});

describe('latest', () => {
    it('emits an updated array on every source emission', () => {
        const received: any[] = [];
        latest([of(1), of('x')]).subscribe((v) => received.push([...v]));
        expect(received.length).toBeGreaterThanOrEqual(2);
    });

    it('the final emission reflects every source\'s latest value', () => {
        const received: any[] = [];
        latest([of(1, 2), of('a', 'b')]).subscribe((v) => received.push([...v]));
        expect(received[received.length - 1]).toEqual([2, 'b']);
    });
});

describe('zip', () => {
    it('pairs values positionally across sources', () => {
        const received: any[] = [];
        zip([of(1, 2, 3), of('a', 'b', 'c')]).subscribe((v) => received.push(v));
        expect(received).toEqual([
            [1, 'a'],
            [2, 'b'],
            [3, 'c'],
        ]);
    });

    it('stops pairing once the shortest source is exhausted', () => {
        const received: any[] = [];
        zip([of(1, 2), of('a', 'b', 'c', 'd')]).subscribe((v) => received.push(v));
        expect(received).toEqual([
            [1, 'a'],
            [2, 'b'],
        ]);
    });
});

describe('sequent', () => {
    it('subscribes to sources one at a time, in order', () => {
        const received: any[] = [];
        sequent([of('a'), of('b'), of('c')]).subscribe((v) => received.push(v));
        expect(received).toEqual(['a', 'b', 'c']);
    });

    it('does not throw when sources complete synchronously', () => {
        expect(() => {
            sequent([of(1), of(2), of(3)]).subscribe(() => { });
        }).not.toThrow();
    });

    it('handles an empty array by completing with no emissions', () => {
        const onComplete = jest.fn();
        sequent([]).subscribe(() => { }, undefined, onComplete);
        expect(onComplete).toHaveBeenCalled();
    });

    it('supports late-subscriber replay of the full sequence', () => {
        const seq = sequent([of('x'), of('y')]);
        const first: any[] = [];
        seq.subscribe((v) => first.push(v));
        const second: any[] = [];
        seq.subscribe((v) => second.push(v));
        expect(first).toEqual(['x', 'y']);
        expect(second).toEqual(['x', 'y']);
    });

    it('propagates an error and stops advancing the queue', () => {
        const onError = jest.fn();
        const onNext = jest.fn();
        sequent([
            of('a'),
            new Observer((_n, error) => error('boom')),
            of('never reached'),
        ]).subscribe(onNext, onError);
        expect(onError).toHaveBeenCalledWith('boom');
        expect(onNext).toHaveBeenCalledTimes(1);
        expect(onNext).toHaveBeenCalledWith('a');
    });
});

describe('race', () => {
    it('mirrors whichever source emits first', () => {
        const received: any[] = [];
        race([of('first'), of('second')]).subscribe((v) => received.push(v));
        expect(received).toEqual(['first']);
    });

    it('only emits once', () => {
        const spy = jest.fn();
        race([of(1), of(2), of(3)]).subscribe(spy);
        expect(spy).toHaveBeenCalledTimes(1);
    });

    it('picks the source that emits first even with async timing', (done) => {
        race([asyncOf('slow', 30), asyncOf('fast', 5)]).subscribe((v) => {
            expect(v).toBe('fast');
            done();
        });
    });

    it('propagates an error from whichever source errors first', () => {
        const onError = jest.fn();
        race([new Observer((_n, error) => error('boom')), of(1)]).subscribe(() => { }, onError);
        expect(onError).toHaveBeenCalledWith('boom');
    });
});

describe('flat', () => {
    it('merges all values from all sources into one stream', () => {
        const received: any[] = [];
        flat([of(1, 2), of(3, 4)]).subscribe((v) => received.push(v));
        expect(received.sort()).toEqual([1, 2, 3, 4]);
    });

    it('completes only after every source has completed', () => {
        const onComplete = jest.fn();
        flat([asyncOf(1, 5), asyncOf(2, 15)]).subscribe(() => { }, undefined, onComplete);
        expect(onComplete).not.toHaveBeenCalled();
    });

    it('handles an empty array by completing with no emissions', () => {
        const onComplete = jest.fn();
        flat([]).subscribe(() => { }, undefined, onComplete);
        expect(onComplete).toHaveBeenCalled();
    });
});