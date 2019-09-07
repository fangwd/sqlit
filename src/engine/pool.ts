import { EventEmitter } from 'events';

const MAX = 10;

interface Options<Connection> {
  max?: number;
  connect: () => Promise<Connection>;
  close: (connection: Connection) => void;
}

interface Request<Connection> {
  resolve: (connection: Connection) => void;
  reject: (reason: any) => void;
}

export default class Pool<Connection> extends EventEmitter {
  options: Options<Connection>;
  connections: Array<Connection> | undefined;
  requests: Array<Request<Connection>>;
  connectionCount: number;

  constructor(options: Options<Connection>) {
    super();
    this.options = { max: MAX, ...options };
    this.connections = [];
    this.connectionCount = 0;
    this.requests = [];
  }

  get closed() {
    return this.connections === undefined;
  }

  acquire(): Promise<Connection> {
    if (this.closed) throw new Error('Pool closed');
    return new Promise<Connection>((resolve, reject) => {
      const request: Request<Connection> = { resolve, reject };
      if (this.connections.length > 0) {
        const connection = this.connections.shift();
        request.resolve(connection);
      } else {
        this.requests.push(request);
        if (this.connectionCount < this.options.max) {
          this.connectionCount++;
          this.allocate();
        }
      }
    });
  }

  private allocate() {
    this.options
      .connect()
      .then(connection => {
        if (this.requests.length > 0) {
          const request = this.requests.shift();
          request.resolve(connection);
        }
      })
      .catch(error => {
        if (this.requests.length > 0) {
          const request = this.requests.shift();
          request.reject(error);
        }
        this.emit('error', error);
      });
  }

  release(connection: Connection) {
    if (this.requests.length > 0) {
      const request = this.requests.shift();
      request.resolve(connection);
    } else if (this.closed) {
      this.options.close(connection);
    } else {
      this.connections.push(connection);
    }
  }

  remove(connection: Connection) {
    const index = this.connections.indexOf(connection);
    if (index !== -1) {
      this.connections.splice(index, 1);
      if (this.requests.length > 0) {
        this.allocate();
      } else {
        this.connectionCount--;
      }
    }
  }

  close(): Promise<any> {
    this.requests.forEach(request => request.reject(new Error('Pool closed')));

    this.requests = [];
    this.connections = undefined;
    this.connectionCount = -1;

    const promises = this.connections.map(connection =>
      this.options.close(connection)
    );

    return Promise.all(promises);
  }
}
