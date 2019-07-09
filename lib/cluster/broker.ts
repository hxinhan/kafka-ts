import { Connection } from '../network/connection'
import { apiVersionsAPI } from '../protocol/apiVersions'
import { SmartBuffer } from 'smart-buffer';
import { INT32_SIZE } from '../protocol/decoder'
import { logger } from '../logger'

export class KafkaBroker {

    private connection: Connection
    private clientId = 'test'
    private outstandingRequests = new Map()
    private correlationId = 0

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
                this.correlationId++
                const apiVersions = apiVersionsAPI.version[<keyof typeof apiVersionsAPI.version>candidateVersion]
                const requestPayload = apiVersions().encode(this.clientId, this.correlationId)

                const response = await this.sendRequest(this.correlationId, requestPayload, apiVersions().decode)
                logger.debug('API versions response: ', response)

                break
            } catch (e) {
                if (e.type !== 'UNSUPPORTED_VERSION') {
                    throw e
                }
            }
        }
    }

    private async sendRequest(correlationId: number, payload: Buffer, decode: (response: Buffer) => {}) {
        return new Promise((resolve) => {
            this.outstandingRequests.set(correlationId, { decode, complete: resolve })
            this.connection.send(payload)
        })
    }

    private onData(data: Buffer) {
        const sb = SmartBuffer.fromBuffer(data)
        const messageSize = sb.readInt32BE()
        const correlationId = sb.readInt32BE()

        const handler = this.outstandingRequests.get(correlationId)
        if (handler) {
            const remainingBytes = sb.toBuffer().slice(INT32_SIZE * 2, messageSize + INT32_SIZE * 2)
            const decodedData = handler.decode(remainingBytes)
            handler.complete(decodedData)
        }
    }

    stop() {
        this.connection.disconnect()
    }
}
