/*
 * @Author: richen
 * @Date: 2020-10-10 15:53:12
 * @LastEditTime: 2020-10-10 18:51:39
 * @Description:
 * @Copyright (c) - <richenlin(at)gmail.com>
 */
import { Koatty } from "koatty";
import helper from "think_lib";
import logger from "think_logger";
import { Etcd3, IOptions } from "etcd3";

interface PluginOptions extends IOptions {
    namespace: string;
}

/**
 * default options
 */
const defaultOptions: PluginOptions = {
    hosts: ["127.0.0.1:2379"],
    namespace: "Koatty"
};

export async function PluginEtcd(options: PluginOptions, app: Koatty) {
    options = options ? helper.extend(defaultOptions, options, true) : defaultOptions;

    //
    const reFreshConfig = function (newConfig: string[]) {
        if (helper.isEmpty(newConfig)) {
            return;
        }
        try {
            const typeConfig: any = {};
            for (const n of newConfig) {
                // tslint:disable-next-line: one-variable-per-declaration
                let type = "config", key = "";
                if (n.includes(".")) {
                    type = n.slice(0, n.indexOf("."));
                    key = n.slice(n.indexOf(".") + 1);
                    if (!type || !key) {
                        continue;
                    }
                }
                let val = newConfig[n];
                if (helper.isJSONStr) {
                    val = JSON.parse(val);
                }
                // tslint:disable-next-line: no-unused-expression
                !typeConfig[type] && (typeConfig[type] = {});
                typeConfig[type][key] = val;
            }
            // override
            let appConfigs = {};
            if (app.setMap) {
                appConfigs = app.getMap("configs") || {};
                appConfigs = helper.extend(appConfigs, typeConfig, true);
                app.setMap("configs", appConfigs);
            }
        } catch (err) {
            logger.error(err.stack || err);
        }
    };

    //
    const initEtcd = async function (opt: PluginOptions) {
        const client = new Etcd3(opt);
        const allFValues = await client.getAll().prefix(opt.namespace).keys().catch((err) => {
            return Promise.reject(err);
        });

        if (!helper.isEmpty(allFValues)) {
            reFreshConfig(allFValues);
        }
        logger.info('Etcd initialization is complete.');
        helper.define(app, "etcdClient", client, true);

        // watcher
        const watcher = await client.watch().prefix(opt.namespace).create().catch((err) => {
            return Promise.reject(err);
        });
        watcher.on('disconnected', () => logger.warn('Etcd initialization is complete.'))
            .on('connected', () => logger.info('Etcd successfully reconnected!'))
            .on('put', (res) => {
                const key = res.key.toString();
                const value = res.value.toString();
                reFreshConfig([`{"${key}":"${value}"}`]);
            });
    };
    helper.define(app, "initEtcd", initEtcd, true);

    return initEtcd(options);
}