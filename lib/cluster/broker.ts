import { Connection } from '../network/connection'
import { apiVersionsAPI } from '../protocol/apiVersions'
import { SmartBuffer } from 'smart-buffer';
import { INT32_SIZE } from '../protocol/decoder'
import { logger } from '../logger'

export class KafkaBroker {

    private connection: Connection
    private clientId = 'test'
    private outstandingRequests = new Map()

    constructor(readonly host: string, readonly port: number) {
        this.connection = new Connection(host, port)
    }

    async connect() {
        await this.connection.connect()
        this.connection.data$().subscribe((data) => this.onData(data))
    }

    isConnected() {
        return this.connection.isConnected()
    }

    async apiVersions() {
        const availableVersions = apiVersionsAPI.versions
            .sort()
            .reverse()

        // Find the best version implemented by the server
        for (let candidateVersion of availableVersions) {
            try {
                const apiVersions = apiVersionsAPI.version[<keyof typeof apiVersionsAPI.version>candidateVersion]

                const correlationId = 1
                const requestPayload = apiVersions().encode(this.clientId, correlationId)
                // this.outstandingRequests.set(correlationId, { decode: apiVersions().decode, completed: Promise })
                // this.connection.send(requestPayload)

                const response = await this.sendRequest(correlationId, requestPayload, apiVersions().decode)
                logger.debug('API versions response: ', response)
                logger.info('!!!!!!!!!!!!!!!!!!!!!!!! resolved')
                // await this.sendRequest(correlationId, requestPayload)

                break
            } catch (e) {
                if (e.type !== 'UNSUPPORTED_VERSION') {
                    throw e
                }
            }
        }
    }

    private async sendRequest(correlationId: number, payload: Buffer, decode: (response: Buffer) => {}) {
        // this.outstandingRequests.set(correlationId)
        return new Promise((resolve) => {
            this.outstandingRequests.set(correlationId, { decode, completed: resolve })
            this.connection.send(payload)
        })
        /*
        const completed = Promise
        this.outstandingRequests.set(correlationId, { decode, completed })
        this.connection.send(payload)
        return completed
        */
    }
    /*
    private async sendRequest(correlationId: number, payload: Buffer) {
        // this.outstandingRequests.set(correlationId)
        this.connection.send(payload)
    }
    */

    private onData(data: Buffer) {
        const sb = SmartBuffer.fromBuffer(data)
        const messageSize = sb.readInt32BE()
        const correlationId = sb.readInt32BE()

        const handler = this.outstandingRequests.get(correlationId)
        if (handler) {
            const remainingBytes = sb.toBuffer().slice(INT32_SIZE * 2, messageSize + INT32_SIZE * 2)
            const decodedData = handler.decode(remainingBytes)
            // logger.debug('DecodedData2: ', JSON.stringify(decodedData, null, 2))
            handler.completed(decodedData)
        }
    }

    stop() {
        this.connection.disconnect()
    }
}
