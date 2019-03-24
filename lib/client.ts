import * as net from 'net'
import { SmartBuffer } from 'smart-buffer'
import { metadataAPI } from './protocol/metadata'
import { apiVersionsAPI } from './protocol/apiVersions'
import { INT32_SIZE } from './protocol/decoder'

const clientName = 'test'
const host = '127.0.0.1'
const port = 9092

const socket = net.createConnection(port, host)
console.log('connecting to 127.0.0.1:9092')

const metadataAPIHandler = metadataAPI['0']()
const apiVersionsAPIHandler = apiVersionsAPI['0']()

const requestQueue = new Map()

socket.on('connect', () => {
    console.log('connected')
    let correlationId = 1

    let request = apiVersionsAPIHandler.encode(clientName, correlationId)
    requestQueue.set(correlationId, apiVersionsAPIHandler)
    socket.write(request)

    correlationId++
    request = metadataAPIHandler.encode(clientName, correlationId, [])
    requestQueue.set(correlationId, metadataAPIHandler)


    socket.write(request)
})
socket.on('data', (data) => {
    const sb = SmartBuffer.fromBuffer(data)
    const messageSize = sb.readInt32BE()
    const correlationId = sb.readInt32BE()

    console.log('======================')
    console.log('messageSize: ', messageSize)
    console.log('correlationId: ', correlationId)

    const handler = requestQueue.get(correlationId)
    console.log(handler)
    if (handler) {
        const remainingBytes = sb.toBuffer().slice(INT32_SIZE * 2, messageSize + INT32_SIZE * 2)
        const decodedData = handler.decode(remainingBytes)
        console.log('decodedData: ', JSON.stringify(decodedData, null, 2))
    }
})
socket.on('error', (err) => {
    console.log('error: ', err)
})
socket.on('close', () => {
    console.log('closed')
})
