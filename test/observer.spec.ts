import { Observer } from '../src/observer';

describe('Observer', () => {
    it('does not invoke its task until subscribed', () => {
        const task = jest.fn(() => undefined);
        new Observer<void>(task);
        expect(task).not.toHaveBeenCalled();
    });

    it('invokes its task on first subscribe', () => {
        const task = jest.fn(() => undefined);
        const obs = new Observer<void>(task);
        obs.subscribe(() => { });
        expect(task).toHaveBeenCalledTimes(1);
    });

    it('delivers synchronously-emitted values to the subscriber', () => {
        const obs = new Observer<number>((next, error, complete) => {
            next(1);
            next(2);
            complete();
        });
        const received: number[] = [];
        obs.subscribe((v) => received.push(v));
        expect(received).toEqual([1, 2]);
    });

    it('delivers error to the error handler', () => {
        const obs = new Observer<number>((next, error) => {
            error('boom');
        });
        const onError = jest.fn();
        obs.subscribe(() => { }, onError);
        expect(onError).toHaveBeenCalledWith('boom');
    });

    it('calls the returned teardown function on unsubscribe', () => {
        const teardown = jest.fn();
        const obs = new Observer<number>(() => teardown);
        const sub = obs.subscribe(() => { });
        sub.unsubscribe();
        expect(teardown).toHaveBeenCalledTimes(1);
    });

    it('does not re-invoke the task for a second subscriber while the context is still active', () => {
        const task = jest.fn(() => undefined);
        const obs = new Observer<void>(task);
        obs.subscribe(() => { });
        obs.subscribe(() => { });
        expect(task).toHaveBeenCalledTimes(1);
    });

    it('replays (re-invokes the task) for a late subscriber after the context has completed', () => {
        let runs = 0;
        const obs = new Observer<number>((next, error, complete) => {
            runs++;
            next(runs);
            complete();
        });

        const first: number[] = [];
        obs.subscribe((v) => first.push(v));

        const second: number[] = [];
        obs.subscribe((v) => second.push(v));

        expect(runs).toBe(2);
        expect(first).toEqual([1]);
        expect(second).toEqual([2]);
    });

    it('replays for a late subscriber after the context has errored', () => {
        let runs = 0;
        const obs = new Observer<number>((next, error) => {
            runs++;
            error('fail ' + runs);
        });

        const firstError = jest.fn();
        obs.subscribe(() => { }, firstError);

        const secondError = jest.fn();
        obs.subscribe(() => { }, secondError);

        expect(runs).toBe(2);
        expect(firstError).toHaveBeenCalledWith('fail 1');
        expect(secondError).toHaveBeenCalledWith('fail 2');
    });

    describe('resubscribing from within an error handler (re-entrancy)', () => {
        it('re-invokes the task rather than joining the same in-flight dispatch', () => {
            let runs = 0;
            const source = new Observer<string>((next, error) => {
                runs++;
                next('value from run ' + runs);
                error('fail on run ' + runs);
            });

            const results: string[] = [];
            const errors: string[] = [];

            source.subscribe(
                (v) => results.push(v),
                (err) => {
                    errors.push(err);
                    if (runs < 2) {
                        // resubscribe synchronously from within the error handler
                        source.subscribe(
                            (v) => results.push(v),
                            (err2) => errors.push(err2)
                        );
                    }
                }
            );

            expect(runs).toBe(2);
            expect(results).toEqual(['value from run 1', 'value from run 2']);
            expect(errors).toEqual(['fail on run 1', 'fail on run 2']);
        });
    });

    describe('synchronous sources', () => {
        it('does not throw when a source completes synchronously during subscribe', () => {
            const obs = new Observer<number>((next, error, complete) => {
                next(1);
                complete();
            });
            expect(() => obs.subscribe(() => { })).not.toThrow();
        });
    });
});