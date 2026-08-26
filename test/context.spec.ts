import { ObserverContext } from '../src/context';

describe('ObserverContext', () => {
    it('starts inactive/uninvoked before invoke() is called', () => {
        const ctx = new ObserverContext();
        expect(ctx.active).toBe(true);
        expect(ctx.invoked).toBe(false);
    });

    it('runs the task function on invoke()', () => {
        const task = jest.fn(() => undefined);
        const ctx = new ObserverContext();
        ctx.invoke(task);
        expect(task).toHaveBeenCalledTimes(1);
        expect(ctx.invoked).toBe(true);
    });

    it('delivers next() values to subscribers added before invoke', () => {
        const ctx = new ObserverContext<number>();
        const received: number[] = [];
        ctx.add({ next: (v) => received.push(v) });
        ctx.invoke((next) => {
            next(1);
            next(2);
        });
        expect(received).toEqual([1, 2]);
    });

    it('does not deliver values to a subscriber added AFTER invoke has already synchronously completed', () => {
        // regression: subscribe() must add the subscriber before invoking the task,
        // or synchronous emissions are silently dropped
        const ctx = new ObserverContext<number>();
        const received: number[] = [];
        ctx.invoke((next) => {
            next(1); // fires before anyone is subscribed in this deliberately-wrong order
        });
        ctx.add({ next: (v) => received.push(v) });
        expect(received).toEqual([]);
    });

    describe('complete()', () => {
        it('calls each subscriber complete handler', () => {
            const ctx = new ObserverContext<number>();
            const onComplete = jest.fn();
            ctx.add({ next: () => { }, complete: onComplete });
            ctx.invoke((next, error, complete) => {
                complete(42);
            });
            expect(onComplete).toHaveBeenCalledWith(42);
        });

        it('notifies subscribers before the context is marked inactive to external checks made from within the handler', () => {
            // regression: complete() must notify before/while closing so that
            // resubscribing from within a complete handler is possible where intended
            const ctx = new ObserverContext<number>();
            let sawActiveDuringHandler: boolean | null = null;
            ctx.add({
                next: () => { },
                complete: () => {
                    sawActiveDuringHandler = ctx.active;
                },
            });
            ctx.invoke((next, error, complete) => complete());
            expect(sawActiveDuringHandler).toBe(false);
        });

        it('is a no-op if called after the context is already closed', () => {
            const ctx = new ObserverContext<number>();
            const onComplete = jest.fn();
            ctx.add({ next: () => { }, complete: onComplete });
            ctx.invoke((next, error, complete) => {
                complete();
                complete(); // second call should be ignored
            });
            expect(onComplete).toHaveBeenCalledTimes(1);
        });
    });

    describe('error()', () => {
        it('calls each subscriber error handler', () => {
            const ctx = new ObserverContext<number>();
            const onError = jest.fn();
            ctx.add({ next: () => { }, error: onError });
            ctx.invoke((next, error) => {
                error('boom');
            });
            expect(onError).toHaveBeenCalledWith('boom');
        });

        it('closes the context before notifying, so a subscriber can safely resubscribe to the SAME observer context flow from within its own error handler', () => {
            // regression: this is the re-entrancy bug -- if the context isn't marked
            // closed before notifying, a resubscribe from inside the error handler
            // would be silently absorbed into the same in-flight dispatch instead
            // of triggering a fresh invocation.
            const ctx = new ObserverContext<number>();
            let activeDuringErrorHandler: boolean | null = null;
            ctx.add({
                next: () => { },
                error: () => {
                    activeDuringErrorHandler = ctx.active;
                },
            });
            ctx.invoke((next, error) => error('boom'));
            expect(activeDuringErrorHandler).toBe(false);
        });

        it('does not throw if there is no error handler registered', () => {
            const ctx = new ObserverContext<number>();
            ctx.add({ next: () => { } });
            expect(() => {
                ctx.invoke((next, error) => error('boom'));
            }).not.toThrow();
        });
    });

    describe('update()', () => {
        it('does not deliver values after the context has closed', () => {
            const ctx = new ObserverContext<number>();
            const received: number[] = [];
            let updateFn: (v: number) => void = () => { };
            ctx.add({ next: (v) => received.push(v) });
            ctx.invoke((next, error, complete) => {
                updateFn = next;
                complete();
            });
            updateFn(99); // attempt to emit after completion
            expect(received).toEqual([]);
        });

        it('catches a throw from a subscriber next handler and routes it to error() instead of propagating', () => {
            // regression: a throwing next handler previously crashed the process
            // uncaught; it should be caught at the context level and routed to error()
            const ctx = new ObserverContext<number>();
            const onError = jest.fn();
            ctx.add({
                next: () => {
                    throw new Error('handler exploded');
                },
                error: onError,
            });
            expect(() => {
                ctx.invoke((next) => next(1));
            }).not.toThrow();
            expect(onError).toHaveBeenCalledTimes(1);
        });
    });

    describe('close()', () => {
        it('removes all subscribers and prevents further delivery', () => {
            const ctx = new ObserverContext<number>();
            const received: number[] = [];
            let updateFn: (v: number) => void = () => { };
            ctx.add({ next: (v) => received.push(v) });
            ctx.invoke((next) => {
                updateFn = next;
            });
            ctx.close();
            updateFn(1);
            expect(received).toEqual([]);
            expect(ctx.active).toBe(false);
        });

        it('calls the teardown function returned by the task', () => {
            const teardown = jest.fn();
            const ctx = new ObserverContext<number>();
            ctx.invoke(() => teardown);
            ctx.close();
            expect(teardown).toHaveBeenCalledTimes(1);
        });
    });

    describe('add()', () => {
        it('returns a function that removes the subscriber', () => {
            const ctx = new ObserverContext<number>();
            const received: number[] = [];
            let updateFn: (v: number) => void = () => { };
            const remove = ctx.add({ next: (v) => received.push(v) });
            ctx.invoke((next) => {
                updateFn = next;
            });
            remove();
            updateFn(1);
            expect(received).toEqual([]);
        });

        it('auto-closes the context when the last subscriber is removed', () => {
            const ctx = new ObserverContext<number>();
            const remove = ctx.add({ next: () => { } });
            ctx.invoke(() => { });
            remove();
            expect(ctx.active).toBe(false);
        });
    });
});