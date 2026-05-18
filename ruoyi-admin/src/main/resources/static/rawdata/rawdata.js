$(function () {
    // 加载目录列表
    loadDirList();

    // 加载年月目录
    function loadDirList() {
        $.ajax({
            url: "/ieims/rawdata/dirs",
            type: "GET",
            dataType: "json",
            success: function (res) {

                console.log(typeof res);
                console.log(res);
                if (res.code === 200 || res.code === 0) {
                    let html = "";
                    if (res.data.length === 0) {
                        html = '<div class="dir-item">暂无目录</div>';
                    } else {

                        // 带索引遍历
                        res.data.forEach((dir, index) => {
                            if (index===0){
                                loadFileList(dir);
                            }
                            html += `<div class="dir-item" data-dir="${dir}" data-index="${index}">${dir}</div>`;
                        });
                    }
                    console.log(html);
                    $("#dirList").html(html);

                    // 绑定目录点击事件
                    $(".dir-item").click(function () {
                        // 切换选中状态
                        $(".dir-item").removeClass("active");
                        $(this).addClass("active");
                        // 加载对应文件列表
                        loadFileList($(this).data("dir"));
                    });
                } else {
                    layer.msg("加载目录失败：" + res.msg);
                }
            },
            error: function (xhr) {
                layer.msg("加载目录失败，请检查权限");
                console.error(xhr);
            }
        });
    }

    // 加载指定目录下的日志文件
    function loadFileList(dir) {
        $.ajax({
            url: "/ieims/rawdata/files/" + dir,
            type: "GET",
            dataType: "json",
            success: function (res) {
                console.log(typeof res);
                console.log(res);
                if (res.code === 200 || res.code === 0) {
                    let html = "";
                    if (res.data.length === 0) {
                        html = `<div style="grid-column: span 2;text-align:center;color:#999;">该目录下暂无日志文件</div>`;
                    } else {
                        res.data.forEach(file => {
                            let downloadUrl = `/ieims/rawdata/download/${dir}/${file}`;
                            html += `
                                <div class="file-item">
                                    <a href="${downloadUrl}" class="file-link" target="_blank">${file}</a>
                                </div>`;
                        });
                    }
                    console.log(html);
                    $("#fileList").html(html);
                } else {
                    layer.msg("加载文件失败：" + res.msg);
                }
            },
            error: function (xhr) {
                layer.msg("加载文件失败，请检查权限");
                console.error(xhr);
            }
        });
    }
});