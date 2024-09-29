import {connect } from '../lib/redisClient';
const config = require('./config');
import {type Action, Message, Receiver} from '@baristaio/rpipe/lib/types';
import { RPipe }  from '../lib/RPipe';
const modules: string[] = ['test1', 'test2', 'test3'];

const createAggregator = (name:string, client: any) => {
    return new RPipe(name, client, {
        prefix: 'aggregator',
        postFix: name
    });
};

const messageGenerator = (name: string, id: number,  action: Action):Message => {
    const receiver: Receiver = {
        name,
        id
    }
   return {
       receiver,
       action
   };
};

// connect(config.redis).then((client:any) =>  {
//     console.log('client connected');
//     const rpipe = createAggregator('test', client);
//     console.log('rpipe created');
//     const message: Message = messageGenerator('test', 2, {type: 'test', payload: {data: 'test'}});
//     rpipe.registerMessages([message]);
//     console.log('messages registered');
//     await rpipe.move('collector', 'processing');
//     await rpipe.move('processing', 'done');
//     await rpipe.move('done', 'failed');
//     console.log('message moved');
// });


async function main() {
    const client = await connect(config.redis);
    const aggregator = createAggregator('test', client);
    const message: Message = messageGenerator('test', 2, {type: 'test', payload: {data: 'test'}});
    await aggregator.registerMessages([message]);
    await aggregator.moveId("2",'collector', 'processing');
    await aggregator.moveId('2', 'processing', 'done');
    await aggregator.moveId('2', 'done', 'failed');

    const messages: Message[] = [];
    for (let i = 0; i < 1000000; i++) {
        messages.push(messageGenerator('test', 3, {type: `test-${i}`, payload: {count: i + 10000000}}));
    }

    const startTime = performance.now();
    await aggregator.registerMessages(messages);
    await aggregator.next("3", 'collector');
    let state: string | null = aggregator.getNextStateName('collector');
    await aggregator.next('3', state as string);
    state = aggregator.getNextStateName(state as string) as string;
    await aggregator.next('3', state as string);
    const endTime = performance.now()
    console.log(`Data moved successfully: ${endTime - startTime} [ms]`);
}


main().catch(console.error);
