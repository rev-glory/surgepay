module.exports = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  rootDir: 'src',
  testRegex: '.*\\.spec\\.ts$',
  transform: {
    '^.+\\.(t|j)s$': 'ts-jest',
  },
  collectCoverageFrom: ['**/*.(t|j)s'],
  coverageDirectory: '../coverage',
  testEnvironment: 'node',
  moduleNameMapper: {
    '^@surgepay/common$': '<rootDir>/../../../packages/common/src',
    '^@surgepay/config$': '<rootDir>/../../../packages/config/src',
    '^@surgepay/contracts$': '<rootDir>/../../../packages/contracts/src',
    '^@surgepay/events$': '<rootDir>/../../../packages/events/src',
    '^@database$': '<rootDir>/../../../packages/database/src',
  },
};
