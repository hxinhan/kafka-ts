import { SmartBuffer } from 'smart-buffer'

export const INT8_SIZE = 1
export const INT16_SIZE = 2
export const INT32_SIZE = 4
export const INT64_SIZE = 8

export class Decoder {
    private smartBuffer: SmartBuffer

    constructor() {
        this.smartBuffer = new SmartBuffer()
    }

    fromBuffer(data: any) {
        this.smartBuffer = SmartBuffer.fromBuffer(data)
    }

    readInt8() {
        return this.smartBuffer.readInt8()
    }

    readInt16() {
        return this.smartBuffer.readInt16BE()
    }

    readInt32() {
        return this.smartBuffer.readInt32BE()
    }

    readString() {
        const byteLength = this.readInt16()

        if (byteLength === -1) {
            return null
        }
        return this.smartBuffer.readString(byteLength)
    }

    readBoolean() {
        return this.readInt8() === 1
    }

    readArray<T>(reader: () => T): T[] {
        const length = this.readInt32()
        if (length === -1) {
            return []
        }

        const array = []
        for (let i = 0; i < length; i++) {
            array.push(reader())
        }

        return array
    }
}