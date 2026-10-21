# Curvine HSM 部署流程图

下载本目录后在浏览器打开 HTML；GitHub 文件预览不会执行交互页面。

- [普通写入自动纳管](write.html)：编号 1—6，含写入流程、Worker 块布局、HSM RocksDB 记录与候选索引。
- [策略筛选与 Release](policy-release.html)：编号 1—11。
- [普通读取自动 Recall](automatic-recall.html)：编号 1—12。

写入说明源文件为 `write-reading.archify.md`；其中的原生部署图为 `write-diagram.html` / `write-diagram.json`。其余两张图各有同名 JSON。HTML 自包含，可以离线打开。

这些页面保留 2026-10-20 验收时的冻结证据标识；其中源码导航指向开发环境内的本地 evidence 快照，不是 GitHub 可访问提交。已发布实现以本仓库的固定 Curvine Git 依赖及 [部署设计](../DEPLOYMENT.md)、[验证记录](../VALIDATION.md)为准。
