import { Connection } from '../network/connection'
import { apiVersionsAPI } from '../protocol/apiVersions'
import { SmartBuffer } from 'smart-buffer'
import { INT32_SIZE } from '../protocol/decoder'
import { logger } from '../logger'
import { REQUEST_TYPE } from '../protocol/request'
import { metadataAPI } from '../protocol/metadata'


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
                const api = apiVersionsAPI.api[<keyof typeof apiVersionsAPI.api>candidateVersion]()
                const payload = api.encode(this.clientId, this.correlationId)

                const response = await this.sendRequest(this.correlationId, payload, api.decode)
                logger.debug('API versions response: ', response)

                for (const apiKey of Object.keys(REQUEST_TYPE).filter((type) => type === 'metadata')) {
                    const apiVersion = response.apiVersions.find(
                        (version) => REQUEST_TYPE[<keyof typeof REQUEST_TYPE>apiKey] === version.apiKey)
                    if (apiVersion) {
                        if (metadataAPI.version > apiVersion.maxVersion) {
                            const version = metadataAPI.versions.find((v) => v >= apiVersion.maxVersion)
                            if (version) {
                                metadataAPI.version = version
                            } else {
                                logger.error(`Broker does not have supported version for API key ${apiKey}`)
                            }
                        } else if (metadataAPI.version < apiVersion.minVersion) {
                            logger.error(`Client does not have supported version for API key ${apiKey}`)
                        }
                    } else {
                        logger.error(`Client does not support API key ${apiKey}`)
                    }
                }
                break
            } catch (e) {
                if (e.type !== 'UNSUPPORTED_VERSION') {
                    throw e
                }
            }
        }
    }

    async metadata() {
        this.correlationId++
        const api = metadataAPI
            .api[<keyof typeof metadataAPI.api>metadataAPI.version]()
        const payload = api.encode(this.clientId, this.correlationId, [])

        const response = await this.sendRequest(this.correlationId, payload, api.decode)
        logger.debug('metadata response: ', response)
    }

    private async sendRequest<T>(correlationId: number, payload: Buffer, decode: (response: Buffer) => T) {
        return new Promise<T>((resolve) => {
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
