import { PartitionMetadata } from './version-0'
import { BrokerMetadataV1, TopicMetadataV1 } from './version-1'
import { MetadataResponseV2, MetadataV2 } from './version-2'

export interface MetadataResponseV3 extends MetadataResponseV2 {
    throttleTimeMs: number
}

export type BrokerMetadataV3 = BrokerMetadataV1
export type TopicMetadataV3 = TopicMetadataV1

export class MetadataV3 extends MetadataV2 {
    constructor() {
        super()
        this.apiVersion = 3
    }

    /**
     * Decode metadata response
     *
     * @param {Buffer} response
     * @returns {MetadataResponseV4}
     * @memberof MetadataV3
     */
    decode(response: Buffer): MetadataResponseV3 {
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
            throttleTimeMs: this.decoder.readInt32(),
            brokers: this.decoder.readArray<BrokerMetadataV3>(brokersReader),
            clusterId: this.decoder.readString(),
            controllerId: this.decoder.readInt32(),
            topics: this.decoder.readArray<TopicMetadataV3>(topicsReader).filter((p) => !p.errorCode)
        }
    }
}