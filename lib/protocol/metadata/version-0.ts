import { API } from '../api'
import { REQUEST_TYPE } from '../request'

export interface MetadataResponseBase {
    brokers: BrokerMetadataBase[]
    topics: TopicMetadataBase[]
}

export interface BrokerMetadataBase {
    nodeId: number
    host: string | null
    port: number
}

export interface TopicMetadataBase {
    errorCode: number
    topic: string
    partitions: PartitionMetadata[]
}

export interface PartitionMetadata {
    errorCode: number
    partition: number
    leader: number
    replicas: number[]
    isr: number[]
}

export class MetadataV0 extends API {
    protected apiVersion: number

    constructor() {
        super()
        this.apiVersion = 0
    }

    protected encodeBase(clientId: string, correlationId: number, params: { topics: string[] }) {
        return this.encodeRequestHeader(clientId, correlationId, REQUEST_TYPE.metadata, this.apiVersion)
            .writeArray<string>(params.topics, (topic: string) =>
                this.encoder.writeInt16(topic.length)
                    .writeString(topic))
    }

    /**
     * Encode metadata request
     *
     * @param {string} clientId
     * @param {number} correlationId
     * @param {{ topics: string[] }} params
     * @returns
     * @memberof MetadataBase
     */
    encode(clientId: string, correlationId: number, params: { topics: string[] }) {
        return this.encodeRequestWithLength(this.encodeBase(clientId, correlationId, params).toBuffer())
    }

    /**
     * Decode metadata response
     *
     * @param {Buffer} response
     * @returns {MetadataResponse}
     * @memberof MetadataBase
     */
    decode(response: Buffer): MetadataResponseBase {
        this.decoder.fromBuffer(response)
        const brokersReader = () => (
            {
                nodeId: this.decoder.readInt32(),
                host: this.decoder.readString(),
                port: this.decoder.readInt32()
            }
        )
        const topicsReader = () => (
            {
                errorCode: this.decoder.readInt16(),
                topic: this.decoder.readString() as string,
                partitions: this.decoder.readArray<PartitionMetadata>(partitionsReader).filter((p) => !p.errorCode)
            }
        )
        const partitionsReader = () => ({
            errorCode: this.decoder.readInt16(),
            partition: this.decoder.readInt32(),
            leader: this.decoder.readInt32(),
            replicas: this.decoder.readArray(() => this.decoder.readInt32()),
            isr: this.decoder.readArray(() => this.decoder.readInt32()),
        })

        return {
            brokers: this.decoder.readArray<BrokerMetadataBase>(brokersReader),
            topics: this.decoder.readArray<TopicMetadataBase>(topicsReader).filter((p) => !p.errorCode)
        }
    }
}
