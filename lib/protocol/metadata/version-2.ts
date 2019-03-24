import { PartitionMetadata } from './version-0'
import { MetadataResponseV1, BrokerMetadataV1, TopicMetadataV1, MetadataV1 } from './version-1'

export interface MetadataResponseV2 extends MetadataResponseV1 {
    clusterId: string | null
}

type BrokerMetadataV2 = BrokerMetadataV1
type TopicMetadataV2 = TopicMetadataV1

export class MetadataV2 extends MetadataV1 {
    constructor() {
        super()
        this.apiVersion = 2
    }

    /**
     * Decode metadata response
     *
     * @param {Buffer} response
     * @returns {MetadataResponseV2}
     * @memberof MetadataV2
     */
    decode(response: Buffer): MetadataResponseV2 {
        this.decoder.fromBuffer(response)
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
            brokers: this.decoder.readArray<BrokerMetadataV2>(brokersReader),
            clusterId: this.decoder.readString(),
            controllerId: this.decoder.readInt32(),
            topics: this.decoder.readArray<TopicMetadataV2>(topicsReader).filter((p) => !p.errorCode)
        }
    }
}
