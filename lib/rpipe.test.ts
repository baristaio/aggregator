// Removed the incorrect import
import { connect } from "./redisClient";
import { RedisClientType } from 'redis';
import { describe, expect, it, beforeAll } from '@jest/globals';
import { RPipe } from './RPipe';
import {Message} from "./types";


const options = {
    host: 'localhost',
    port: 6379
};

describe('Aggregator', () => {
    let redisClient: RedisClientType;
    let aggregator: RPipe;

    beforeAll(async () => {
        redisClient = await connect(options);
        aggregator = new RPipe('testAggregator', redisClient, {
            states: ['processing', 'done', 'failed'],
            postFix: 'testAggregator'
        });
    });

    it('should return the correct collector name', () => {
        expect(aggregator.getCollectoreName()).toBe('collector');
    });

    it('should register messages correctly', async () => {
        const messages:  Message[] = [
          {receiver: {id: '123', name: 'test' }, action: {type: 'testAction'}}
        ];
        await aggregator.registerMessages(messages);
        // Verify the message was added to the correct set
        const members = await redisClient.sMembers('rpipe:group:testAggregator:id:123:state:collector:testAggregator');
        expect(members).toContain(JSON.stringify({type:"testAction"}));
    });

    it('should throw an error for invalid messages', async () => {
        const invalidMessages = [{ receiver: { id: '123' }, action: { type: '' } }]; // Assuming this is invalid
        await expect(aggregator.registerMessages(invalidMessages as Message[])).rejects.toThrow('Invalid message');
    });

    it('should correctly generate a Redis key', () => {
        const key = aggregator.getKey('123', 'processing');
        expect(key).toBe('rpipe:group:testAggregator:id:123:state:processing:testAggregator');
    });

    it('should throw an error when generating a key with an invalid state', () => {
        expect(() => aggregator.getKey('123', 'invalidState')).toThrow('Invalid state name - invalidState');
    });

    it('should parse a Redis key into its constituent parts', () => {
        const parsedKey = aggregator.parseKey('rpipe:group:testAggregator:id:123:state:processing:testAggregator');
        expect(parsedKey).toEqual({ id: '123', state: 'processing' });
    });

    it('should throw an error when parsing an invalid Redis key format', () => {
        expect(() => aggregator.parseKey('invalidKeyFormat')).toThrow('Invalid key format');
    });

    it('should return the next state name correctly', () => {
        const nextState = aggregator.getNextStateName('processing');
        expect(nextState).toBe('done');
    });

    it('should return null when requesting next state name for the last state', () => {
        const nextState = aggregator.getNextStateName('failed');
        expect(nextState).toBeNull();
    });

    it('should throw an error when requesting next state name for an invalid state', () => {
        expect(() => aggregator.getNextStateName('invalidState')).toThrow('Invalid source state name');
    });

    it('should return the correct list of configured states', () => {
        const states = aggregator.states();
        expect(states).toEqual(['collector', 'processing', 'done', 'failed']);
    });

    it('should add a value to the set stored at a key representing a specific state', async () => {
        await aggregator.add('123', 'processing', 'value1');
        const members = await aggregator.getMembers('123', 'processing');
        expect(members).toContain('value1');
    });

    it('should retrieve all members of the set stored at a key representing a specific state', async () => {
        await aggregator.add('123', 'done', 'value2');
        const members = await aggregator.getMembers('123', 'done');
        expect(members).toEqual(expect.arrayContaining(['value2']));
    });

    it('should clear all data associated with a specific identifier and state', async () => {
        await aggregator.add('123', 'failed', 'value3');
        await aggregator.clear('123', 'failed');
        const members = await aggregator.getMembers('123', 'failed');
        expect(members).toEqual([]);
    });

    it('should throw an error when clearing data with an invalid state', async () => {
        await expect(aggregator.clear('123', 'invalidState')).rejects.toThrow('Invalid state name');
    });


    it('should move data from one Redis key to another', async () => {
        // Setup initial state
        await redisClient.sAdd('fromKey', 'value1');
        await aggregator.move('fromKey', 'toKey');
        // Verify data was moved
        const members = await redisClient.sMembers('toKey');
        expect(members).toContain('value1');
    });

    it('should move data from one state to another for a given identifier', async () => {
        // Setup initial state
        await aggregator.add('123', 'processing', 'value2');
        await aggregator.moveId('123', 'processing', 'done');
        // Verify data was moved
        const members = await aggregator.getMembers('123', 'done');
        expect(members).toContain('value2');
    });

    it('should move data to its next state based on the current state', async () => {
        // Setup initial state
        await aggregator.add('123', 'processing', 'value3');
        await aggregator.next('123', 'processing');
        // Verify data was moved to the next state
        const members = await aggregator.getMembers('123', 'done');
        expect(members).toContain('value3');
    });

    it('should retrieve all data in the "collected" state', async () => {
        // Setup initial state
        await aggregator.add('123', 'collector', 'value4');
        const collected = await aggregator.getCollected('123');
        // Verify data was retrieved
        expect(collected).toContain('value4');
    });

    it('should combine data from multiple states into a single state', async () => {
        // Setup initial state
        await aggregator.add('123', 'processing', 'value5');
        await aggregator.add('123', 'done', 'value6');
        await aggregator.merge('123', 'collector', ['processing', 'done']);
        // Verify data was combined
        const members = await aggregator.getMembers('123', 'collector');
        expect(members).toEqual(expect.arrayContaining(['value5', 'value6']));
    });
});

