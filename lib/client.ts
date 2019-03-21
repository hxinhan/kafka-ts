import * as net from 'net'
import { metadataAPI } from './protocol/metadata'
// import { apiVersionsAPI } from './protocol/apiVersions'

const clientName = 'test'
const host = '127.0.0.1'
const port = 9092

const socket = net.createConnection(port, host)
console.log('connecting to 127.0.0.1:9092')

const metadataAPIHandler = metadataAPI['1']()

socket.on('connect', () => {
    console.log('connected')
    // const request = apiVersionsAPI[0].encode(clientName, 100)
    // const request = metadataAPI[4].encode(clientName, 100, [])
    const request = metadataAPIHandler.encode(clientName, 100, [])
    console.log('request: ', request)
    socket.write(request)
})
socket.on('data', (data) => {
    // const decodedData = apiVersionsAPI[0].decode(data)
    console.log(data)
    const decodedData = metadataAPIHandler.decode(data)
    console.log('decodedData: ', JSON.stringify(decodedData, null, 2))
})
socket.on('error', (err) => {
    console.log('error: ', err)
})
socket.on('close', () => {
    console.log('closed')
})
