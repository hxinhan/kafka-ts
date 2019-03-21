import { SmartBuffer } from 'smart-buffer'

export class Encoder {
    private readonly smartBuffer: SmartBuffer

    constructor() {
        this.smartBuffer = new SmartBuffer()
    }

    writeInt8(value: number) {
        this.smartBuffer.writeInt8(value)
        return this
    }

    writeInt16(value: number) {
        this.smartBuffer.writeInt16BE(value)
        return this
    }

    writeInt32(value: number) {
        this.smartBuffer.writeInt32BE(value)
        return this
    }

    writeString(value: string) {
        this.smartBuffer.writeString(value)
        return this
    }

    writeArray<T>(items: T[], writer: (item: T) => {}) {
        this.writeInt32(items.length)
        items.forEach((item: T) => writer(item))
        return this
    }

    writeBoolean(value: boolean) {
        if (value) {
            this.writeInt8(1)
        } else {
            this.writeInt8(0)
        }
        return this
    }

    toBuffer() {
        return this.smartBuffer.toBuffer()
    }
}