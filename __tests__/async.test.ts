import { complianceTestsAsync } from "@keeveestore/test-suite";
import { StoreAsync } from "../src";

complianceTestsAsync(
	() =>
		StoreAsync.new<string, string>({
			tableName: "keeveestore",
			connection: {
				apiVersion: "2018-05-12",
				accessKeyId: "accessKeyId",
				secretAccessKey: "secretAccessKey",
				region: "eu-north-1",
				endpoint: "http://localhost:8000",
			},
		}),
	{
		key1: "value1",
		key2: "value2",
		key3: "value3",
		key4: "value4",
		key5: "value5",
	},
);
