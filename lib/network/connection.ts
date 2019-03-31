import * as net from 'net'
import { Subject } from 'rxjs'
import { logger } from '../utils'

export class Connection {

    private socket: net.Socket
    private connected = false
    private dataSubject = new Subject<Buffer>()

    constructor(
        private readonly host: string,
        private readonly port: number,
        private readonly socketTimout = 2 * 60 * 1000,
        private readonly keepAlievTimeout = 60 * 1000) {
        this.socket = new net.Socket()
        this.socket.setTimeout(this.socketTimout)
        this.socket.setKeepAlive(true, this.keepAlievTimeout)
    }

    async connect() {
        logger.debug(`Connecting to ${this.host}:${this.port}`)
        return new Promise((resolve, reject) => {
            this.socket.connect(this.port, this.host)

            this.socket.on('connect', () => {
                logger.debug('Connected.')
                this.connected = true
                resolve()
            })
            this.socket.on('data', (data: Buffer) => this.processData(data))
            this.socket.on('error', (err) => {
                logger.error('Socket on error', err)
                this.disconnect()
                reject(err)
            })
            this.socket.on('timeout', () => {
                logger.error('Socket on timeout')
                this.disconnect()
                reject(new Error('Socket timeout'))
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

    async disconnect() {
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

    private processData(data: Buffer) {
        this.dataSubject.next(data)
    }
}