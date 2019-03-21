import { PartitionMetadata, MetadataV0 } from './version-0'
import { BrokerMetadataV1, TopicMetadataV1 } from './version-1'
import { MetadataResponseV3 } from './version-3'

type MetadataResponseV4 = MetadataResponseV3
type BrokerMetadataV4 = BrokerMetadataV1
type TopicMetadataV4 = TopicMetadataV1

export class MetadataV4 extends MetadataV0 {
    constructor() {
        super()
        this.apiVersion = 4
    }

    /**
     * Encode metadata request
     *
     * @param {string} clientId
     * @param {number} correlationId
     * @param {{ topics: string[], allowAutoTopicCreation: boolean }} params
     * @returns
     * @memberof MetadataV4
     */
    encode(clientId: string, correlationId: number, params: { topics: string[], allowAutoTopicCreation: boolean }) {
        const request = super.encodeBase(clientId, correlationId, params)
        request.writeBoolean(params.allowAutoTopicCreation)
        return this.encodeRequestWithLength(request.toBuffer())
    }

    /**
     * Decode metadata response
     *
     * @param {Buffer} response
     * @returns {MetadataResponseV4}
     * @memberof MetadataV4
     */
    decode(response: Buffer): MetadataResponseV4 {
        const baseResponse = this.decodeResponse(response)
        const brokersReader = () => (
            {
                nodeId: this.decoder.readInt32(),
                host: this.decoder.readString(),
                port: this.decoder.readInt32(),
                rack: this.decoder.readString()
            }
        )
        const topicsReader = () => (
            {
                errorCode: this.decoder.readInt16(),
                topic: this.decoder.readString() as string,
                isInternal: this.decoder.readBoolean(),
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
            ...baseResponse,
            throttleTimeMs: this.decoder.readInt32(),
            brokers: this.decoder.readArray<BrokerMetadataV4>(brokersReader),
            clusterId: this.decoder.readString(),
            controllerId: this.decoder.readInt32(),
            topics: this.decoder.readArray<TopicMetadataV4>(topicsReader).filter((p) => !p.errorCode)
        }
    }
}