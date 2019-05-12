import { IKeyValueStoreAsync } from "@keeveestore/keeveestore";
import { DynamoDB } from "aws-sdk";
// tslint:disable-next-line: no-submodule-imports
import { GetItemOutput, ScanOutput } from "aws-sdk/clients/dynamodb";

export class StoreAsync<K, T> implements IKeyValueStoreAsync<K, T> {
	private constructor(
		private readonly store: DynamoDB,
		private readonly opts: {
			tableName: string;
			connection: DynamoDB.ClientConfiguration;
		},
	) {}

	public static async new<K, T>(opts: {
		tableName: string;
		connection: DynamoDB.ClientConfiguration;
	}): Promise<StoreAsync<K, T>> {
		const store = new DynamoDB(opts.connection);

		// @TODO: remove
		try {
			await store
				.createTable({
					AttributeDefinitions: [
						{
							AttributeName: "ItemKey",
							AttributeType: "S",
						},
					],
					KeySchema: [
						{
							AttributeName: "ItemKey",
							KeyType: "HASH",
						},
					],
					ProvisionedThroughput: {
						ReadCapacityUnits: 1,
						WriteCapacityUnits: 1,
					},
					TableName: opts.tableName,
					StreamSpecification: {
						StreamEnabled: false,
					},
				})
				.promise();
			// tslint:disable-next-line: no-empty
		} catch (error) {}

		return new StoreAsync<K, T>(store, opts);
	}

	public async all(): Promise<[K, T][]> {
		const rows: ScanOutput = await this.store
			.scan({
				TableName: this.opts.tableName,
			})
			.promise();

		if (!rows.Items) {
			return [];
		}

		return rows.Items.map(row => [row.ItemKey.S as any, row.ItemValue.S as any]);
	}

	public async keys(): Promise<K[]> {
		return (await this.all()).map(row => row[0]);
	}

	public async values(): Promise<T[]> {
		return (await this.all()).map(row => row[1]);
	}

	public async get(key: K): Promise<T | undefined> {
		const row: GetItemOutput = await this.store
			.getItem({
				TableName: this.opts.tableName,
				Key: { ItemKey: DynamoDB.Converter.input(key) },
			})
			.promise();

		if (!row.Item) {
			return undefined;
		}

		return row ? ((row.Item.ItemValue.S as unknown) as T) : undefined;
	}

	public async getMany(keys: K[]): Promise<(T | undefined)[]> {
		return Promise.all([...keys].map(async (key: K) => this.get(key)));
	}

	public async pull(key: K): Promise<T | undefined> {
		const item: T | undefined = await this.get(key);

		await this.forget(key);

		return item;
	}

	public async pullMany(keys: K[]): Promise<(T | undefined)[]> {
		const items: (T | undefined)[] = await this.getMany(keys);

		await this.forgetMany(keys);

		return items;
	}

	public async put(key: K, value: T): Promise<boolean> {
		if (await this.has(key)) {
			await this.forget(key);
		}

		await this.store
			.putItem({
				Item: {
					ItemKey: DynamoDB.Converter.input(key),
					ItemValue: DynamoDB.Converter.input(value),
				},
				TableName: this.opts.tableName,
			})
			.promise();

		return this.has(key);
	}

	public async putMany(values: [K, T][]): Promise<boolean[]> {
		return Promise.all(values.map(async (value: [K, T]) => this.put(value[0], value[1])));
	}

	public async has(key: K): Promise<boolean> {
		try {
			return (await this.get(key)) !== undefined;
		} catch (error) {
			return false;
		}
	}

	public async hasMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.has(key)));
	}

	public async missing(key: K): Promise<boolean> {
		return !(await this.has(key));
	}

	public async missingMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map(async (key: K) => this.missing(key)));
	}

	public async forget(key: K): Promise<boolean> {
		if (await this.missing(key)) {
			return false;
		}

		await this.store
			.deleteItem({
				TableName: this.opts.tableName,
				Key: { ItemKey: DynamoDB.Converter.input(key) },
			})
			.promise();

		return this.missing(key);
	}

	public async forgetMany(keys: K[]): Promise<boolean[]> {
		return Promise.all([...keys].map((key: K) => this.forget(key)));
	}

	public async flush(): Promise<boolean> {
		await this.forgetMany(await this.keys());

		return this.isEmpty();
	}

	public async count(): Promise<number> {
		return (await this.keys()).length;
	}

	public async isEmpty(): Promise<boolean> {
		return (await this.count()) === 0;
	}

	public async isNotEmpty(): Promise<boolean> {
		return !(await this.isEmpty());
	}
}
