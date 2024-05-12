import { Kafka } from 'kafkajs'
import { faker } from '@faker-js/faker'

const BROKER_1 = process.env.BROKER_1 || 'localhost:9092'
const BROKER_2 = process.env.BROKER_2 || 'localhost:9092'
const BROKER_3 = process.env.BROKER_3 || 'localhost:9092'

const kafka = new Kafka({
  clientId: 'product-consumer',
  brokers: [BROKER_1, BROKER_2, BROKER_3],
})

const producer = kafka.producer()

const genProduct = () => ({
  id: 120 + faker.number.int(150),
  amount: faker.number.int(40),
  type: faker.number.int(1) === 1 ? 'IN' : 'OUT',
})

const produce = (amount = 1) => new Array(amount).fill(0).map(genProduct)

const produceProduct = async (products) => {
  await producer.connect()
  await producer.send({
    topic: 'stock',
    messages: products.map((product) => ({
      value: JSON.stringify(product),
    })),
  })
  console.log(
    products.map((p) => [p.amount, p.type, p.id].join(' ')).join('\n')
  )
  await producer.disconnect()
}

await produceProduct(produce(10))
