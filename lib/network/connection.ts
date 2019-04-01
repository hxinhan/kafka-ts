import * as net from 'net'
import { Subject } from 'rxjs'
import { logger } from '../logger'
import { ConnectionError } from '../error'

const CONNECTION_TIMEOUT = 10 * 1000
const SOCKET_TIMEOUT = 2 * 60 * 1000
const KEEP_ALIVE_TIMEOUT = 60 * 1000

export class Connection {

    private socket: net.Socket
    private connected = false
    private dataSubject = new Subject<Buffer>()
    private connectionTimeout?: NodeJS.Timer

    constructor(
        private readonly host: string,
        private readonly port: number,
        private readonly socketTimout = SOCKET_TIMEOUT,
        private readonly keepAlievTimeout = KEEP_ALIVE_TIMEOUT) {
        this.socket = new net.Socket()
        this.socket.setTimeout(this.socketTimout)
        this.socket.setKeepAlive(true, this.keepAlievTimeout)
    }

    private onConnect() {
        logger.debug('Connected.')

        this.cleanConnectionTimeout()
        this.connected = true
    }

    private onError(error: Error) {
        logger.error('Socket on error', error)

        this.cleanConnectionTimeout()
        this.disconnect()

        throw new ConnectionError(`Connection error: ${error.message}`, { broker: `${this.host}:${this.port}` })
    }

    private onTimeout() {
        logger.error('Socket on timeout')

        this.cleanConnectionTimeout()
        this.disconnect()

        throw new ConnectionError('Connection timeout', { broker: `${this.host}:${this.port}` })
    }

    private onData(data: Buffer) {
        this.dataSubject.next(data)
    }

    private cleanConnectionTimeout() {
        logger.debug('Clearing connection timeout')

        if (this.connectionTimeout) {
            clearTimeout(this.connectionTimeout)
        }
    }

    async connect() {
        logger.debug(`Connecting to ${this.host}:${this.port}`)

        this.connectionTimeout = setTimeout(() => this.onTimeout(), CONNECTION_TIMEOUT)

        return new Promise((resolve) => {
            this.socket.connect(this.port, this.host)

            this.socket.on('error', (error: Error) => this.onError(error))
            this.socket.on('timeout', () => this.onTimeout())
            this.socket.on('data', (data: Buffer) => this.onData(data))
            this.socket.on('connect', () => {
                this.onConnect()
                resolve()
            })
        })
    }

    pause() {
        this.socket.pause()
    }

    resume() {
        this.socket.resume()
    }

    send(data: Buffer) {
        this.socket.write(data)
    }

    disconnect() {
        logger.debug(`Disconnecting from ${this.host}:${this.port}`)

        this.socket.end()
        this.socket.unref()
        this.connected = false
    }

    isConnected() {
        return this.connected
    }

    data$() {
        return this.dataSubject
    }
}