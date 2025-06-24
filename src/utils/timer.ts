export default async function timer(ms: number) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(undefined);
    }, ms);
  });
}
