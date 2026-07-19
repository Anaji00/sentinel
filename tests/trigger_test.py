import asyncio, sys
from shared.kafka import SentinelProducer

async def main():
    p = SentinelProducer()
    await p.start()
    await p.send('agents.intel.briefs', {'brief': {'headline_summary': 'Global supply chain disruption', 'entities': [{'name': 'Global Shipping', 'type': 'sector', 'role': 'subject'}]}})
    await p.close()
    print('Sent successfully')

if __name__ == '__main__':
    if sys.platform == 'win32': 
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
