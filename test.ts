const bankResult: [number, number, number][] = [];

async function sum(a: number, b: number): Promise<number> {
	// Simulasi async operation (misalnya database query)
	await new Promise((resolve) => setTimeout(resolve, 100));

	const isAlreadyInBankResult = bankResult.find(
		(item) => item[0] === a && item[1] === b
	);

	if (isAlreadyInBankResult) {
		console.log(
			`Cache hit for ${a} + ${b} = ${isAlreadyInBankResult[2]}`
		);
		return isAlreadyInBankResult[2];
	}

	const res = a + b;
	bankResult.push([a, b, res]);
	console.log(`Calculated and cached: ${a} + ${b} = ${res}`);
	console.log("Current bankResult:", bankResult);

	return res;
}

const data = [
	[1, 2],
	[3, 4],
	[5, 6],
	[1, 2], // Ini akan menggunakan cache
	[3, 4], // Ini juga akan menggunakan cache
];

// Solusi 1: Menggunakan for...of loop (Sequential)
(async () => {
	console.log("=== Sequential Processing ===");

	for (const [index, item] of data.entries()) {
		try {
			const result = await sum(item[0], item[1]);
			console.log(`Item ${index} result: ${result}\n`);
		} catch (error) {
			console.error(`Item ${index} error:`, error);
		}
	}
})();

// Solusi 2: Menggunakan reduce untuk sequential processing
(async () => {
	console.log("\n=== Sequential Processing with Reduce ===");

	const results = await data.reduce(async (previousPromise, item, index) => {
		const acc = await previousPromise;

		try {
			const result = await sum(item[0], item[1]);
			console.log(`Item ${index} result: ${result}\n`);
			acc.push({ index, status: "fulfilled", value: result });
		} catch (error) {
			console.error(`Item ${index} error:`, error);
			acc.push({ index, status: "rejected", reason: error });
		}

		return acc;
	}, Promise.resolve([] as any[]));

	console.log("Final results:", results);
})();

// Solusi 3: Jika Anda tetap ingin menggunakan Promise.allSettled tapi dengan delay
(async () => {
	console.log("\n=== Concurrent Processing (Original Approach) ===");

	const promises = data.map(async (item, index) => {
		// Tambahkan delay berdasarkan index untuk simulasi sequential
		await new Promise((resolve) => setTimeout(resolve, index * 200));
		return sum(item[0], item[1]);
	});

	const results = await Promise.allSettled(promises);

	results.forEach((result, index) => {
		if (result.status === "fulfilled") {
			console.log(`Item ${index} result:`, result.value);
		} else {
			console.error(`Item ${index} error:`, result.reason);
		}
	});
})();
