import { Subscription } from '../src/subscription';

describe('Subscription', () => {
    it('starts unclosed', () => {
        const sub = new Subscription();
        expect(sub.closed).toBe(false);
    });

    it('marks itself closed after unsubscribe', () => {
        const sub = new Subscription();
        sub.unsubscribe();
        expect(sub.closed).toBe(true);
    });

    it('calls the provided teardown function on unsubscribe', () => {
        const teardown = jest.fn(() => true);
        const sub = new Subscription(teardown);
        sub.unsubscribe();
        expect(teardown).toHaveBeenCalledTimes(1);
    });

    it('is idempotent -- calling unsubscribe twice only tears down once', () => {
        const teardown = jest.fn(() => true);
        const sub = new Subscription(teardown);
        sub.unsubscribe();
        sub.unsubscribe();
        expect(teardown).toHaveBeenCalledTimes(1);
    });

    it('defaults to a no-op teardown that does not throw', () => {
        const sub = new Subscription();
        expect(() => sub.unsubscribe()).not.toThrow();
    });

    describe('parent/child linking', () => {
        it('cascades unsubscribe from parent to linked child', () => {
            const childTeardown = jest.fn(() => true);
            const parent = new Subscription();
            const child = new Subscription(childTeardown);

            parent.add(child);
            parent.unsubscribe();

            expect(child.closed).toBe(true);
            expect(childTeardown).toHaveBeenCalledTimes(1);
        });

        it('supports a child being linked to multiple parents', () => {
            const childTeardown = jest.fn(() => true);
            const parentA = new Subscription();
            const parentB = new Subscription();
            const child = new Subscription(childTeardown);

            parentA.add(child);
            parentB.add(child);

            parentA.unsubscribe();

            expect(child.closed).toBe(true);
            expect(childTeardown).toHaveBeenCalledTimes(1);
        });

        it('removes itself from all parents when unsubscribed directly', () => {
            const parentA = new Subscription();
            const parentB = new Subscription();
            const child = new Subscription();

            parentA.add(child);
            parentB.add(child);
            child.unsubscribe();

            expect(child.parents).toBeNull();
        });

        it('adding an already-closed subscription unsubscribes it immediately', () => {
            const teardown = jest.fn(() => true);
            const parent = new Subscription();
            const child = new Subscription(teardown);

            child.unsubscribe();
            teardown.mockClear();

            parent.add(child);

            // already closed before being added -- add() should not re-trigger teardown
            expect(teardown).not.toHaveBeenCalled();
        });

        it('adding a subscription to an already-closed parent unsubscribes the child immediately', () => {
            const teardown = jest.fn(() => true);
            const parent = new Subscription();
            const child = new Subscription(teardown);

            parent.unsubscribe();
            parent.add(child);

            expect(child.closed).toBe(true);
            expect(teardown).toHaveBeenCalledTimes(1);
        });

        it('does not throw when unsubscribing a subscription whose teardown was already run indirectly', () => {
            // simulates a subscriber being cascaded-unsubscribed via a shared context,
            // then the aggregate root also trying to unsubscribe it -- should be a safe no-op
            const parent = new Subscription();
            const child = new Subscription();

            parent.add(child);
            child.unsubscribe();

            expect(() => parent.unsubscribe()).not.toThrow();
        });
    });

    describe('unique refs', () => {
        it('generates a unique ref per instance', () => {
            const a = new Subscription();
            const b = new Subscription();
            expect(a.ref).not.toBe(b.ref);
        });
    });
});