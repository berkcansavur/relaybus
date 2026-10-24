import { Module } from '@nestjs/common';

/**
 * Carries the contract of `RelayBusModule.forFeature(...)` into the module that imports it. The
 * contract is checked against that module's providers when the application initialises.
 */
@Module({})
export class RelayBusFeatureModule {}
