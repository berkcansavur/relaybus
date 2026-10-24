export default {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'scope-enum': [
      2,
      'always',
      [
        'core',
        'nestjs',
        'postgres',
        'mongodb',
        'kafka',
        'rabbitmq',
        'testing',
        'eslint-plugin',
        'repo',
        'ci',
        'deps',
        'release',
      ],
    ],
    // Squash-merge bodies and dependency bump bodies contain long lines.
    'body-max-line-length': [0],
    'footer-max-line-length': [0],
  },
};
