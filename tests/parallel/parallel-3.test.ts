import { expect } from 'chai';
import { pause } from '@services/utils';

describe('Parallel - 3 @grepthis', function () {
  it('should run in parallel', async function () {
    await pause(3);
    expect(1).to.equal(1);
  });
});
