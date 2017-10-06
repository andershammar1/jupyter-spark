var UPDATE_FREQUENCY = 10000; // ms
var UPDATE_FREQUENCY_ACTIVE = 500;
var PROGRESS_COUNT_TEXT = "Running Spark job ";

/*
cache is an array of application objects with an added property jobs.
application.jobs is the result of the /applications/applicationId/jobs
API request.
*/
var cache = [];
var current_update_frequency;

var spark_is_running = false;
var cell_queue = [];
var current_cell;
var jobs_in_cache = 0;

var update = function(proxy_url) {
    update_cache(proxy_url, update_dialog_contents);
};

// callbacks follows jQuery callback style, can be either single function or array of functions
// callbacks will be passed the cache as a parameter
var update_cache = function(proxy_url, callbacks) {
    var cbs;
    if (callbacks) {
        cbs = $.Callbacks();
        cbs.add(callbacks);
    }
    $.getJSON(proxy_url + '/api/v1/applications').done(function(applications) {
        var num_applications = cache.length;
        var num_completed = 0;
        // Check if Spark is running before processing applications
        if(!applications.hasOwnProperty('error')){
            spark_is_running = true;
            applications.forEach(function(application, i) {
                $.getJSON(proxy_url + '/api/v1/applications/' + application.id + '/jobs').done(function (jobs) {
                    cache[i] = application;
                    cache[i].jobs = jobs;
                    cache[i].jobs.forEach(function(job) {
                        job.url = proxy_url + '/jobs/job?id=' + job.jobId;
                    });

                    num_completed++;
                    if (num_completed === num_applications && cbs) {
                        cbs.fire(cache);
                    };
                    // Update progress bars if jobs have been run and there are cells to be updated
                    if (jobs.length > jobs_in_cache && cell_queue.length > 0 ) {
                        $(document).trigger('update.progress.bar');
                    };
                });
            });
        } else {
            spark_is_running = false;
        }
    });
};

var update_dialog_contents = function () {
    var element = $('<div/>').attr('id', 'dialog-contents');
    if (cache.length) {
        cache.forEach(function (application, i) {
            element.append(create_application_table(application));
        });
    } else {
        element.text("There are no running Spark jobs.")
    }
    $('#dialog-contents').replaceWith(element);
};

var create_application_table = function(e) {
    var application_div = $('<div/>');
    application_div.append($('<h5/>').text(e.name + ': ' + e.id));
    var application_table = $('<table/>').addClass('table table-hover');

    var header_row = $('<tr/>');
    header_row.append($('<th/>').text('Job ID'));
    header_row.append($('<th/>').text('Job Name'));
    header_row.append($('<th/>').text('Progress'));
    application_table.append(header_row);

    e.jobs.forEach(function(job, i) {
        application_table.append(create_table_row(job));
    });

    application_div.append(application_table);
    return application_div;
};

var create_table_row = function(e) {
    var row = $('<tr/>');
    row.append($('<td/>').text(e.jobId));
    row.append($('<td/>').append($('<a target="_blank"></a>').attr('href', e.url).text(e.name)));

    var status_class = get_status_class(e.status);

    var progress_bar_div = create_progress_bar(status_class, e.numCompletedTasks, e.numTasks);

    row.append($('<td/>').append(progress_bar_div));
    return row;
};

var get_status_class = function(status) {
    var status_class;
    switch(status) {
        case 'SUCCEEDED':
            status_class = 'progress-bar-success';
            break;
        case 'RUNNING':
            status_class = 'progress-bar-info';
            break;
        case 'FAILED':
            status_class = 'progress-bar-danger';
            break;
        case 'UNKNOWN':
            status_class = 'progress-bar-warning';
            break;
    }
    return status_class;
}

var create_progress_bar = function(status_class, completed, total) {
    // progress defined in percent
    var progress = completed / total * 100;

    var progress_bar_div = $('<div/>')
        .addClass('progress')
        .css({'min-width': '100px', 'margin-bottom': 0});
    var progress_bar = $('<div/>')
        .addClass('progress-bar ' + status_class)
        .attr('role', 'progressbar')
        .attr('aria-valuenow', progress)
        .attr('aria-valuemin', 0)
        .attr('aria-valuemax', 100)
        .css('width', progress + '%')
    if (status_class == 'progress-bar-warning') {
        progress_bar.text('Loading Spark...');
    } else {
        progress_bar.text(completed + ' out of ' + total + ' tasks');
    };
    progress_bar_div.append(progress_bar);
    return progress_bar_div;
};


define([
    'jquery',
    'base/js/dialog',
    'base/js/events',
    'base/js/utils',
    'notebook/js/codecell'
], function ($, dialog, events, utils, codecell) {
    var CodeCell = codecell.CodeCell;
    var base_url = utils.get_body_data('baseUrl') || '/';
    var proxy_url = base_url + 'spark';

    var show_running_jobs = function() {
        var element = $('<div/>').attr('id', 'dialog-contents');
        var modal = dialog.modal({
            title: "Running Spark jobs",
            body: element,
            buttons: {
                "Close": {}
            },
            open: update_dialog_contents
        });
    };

    var cancel_running_job = function (jobId) {
        dialog.modal({
            title: "Cancel job",
            body: "Are you sure you want to cancel job " + jobId + "?",
            buttons: {
                "Cancel": {},
                "OK": {
                    class: "btn-primary",
                    click: function () { $.get(proxy_url + "/stages/stage/kill/?id=" + jobId) }
                },
            }
        })
    }

    var spark_progress_bar = function (event, data) {
        var cell = data.cell;
        if (is_spark_cell(cell)) {
            window.clearInterval(current_update_frequency);
            current_update_frequency = window.setInterval(update, UPDATE_FREQUENCY_ACTIVE, proxy_url);
            cell_queue.push(cell);
            current_cell = cell_queue[0];
            add_progress_bar(current_cell);
        };
    };

    var add_progress_bar = function(cell) {
        var progress_bar_div = cell.element.find('.progress-container');
        if (progress_bar_div.length < 1) {
            var input_area = cell.element.find('.input_area');
            if (spark_is_running) {
                jobs_in_cache = cache[0].jobs.length;
            };
            var jobs_completed_container = $('<div/>')
                .addClass('progress_counter')
                .css({'border': 'none', 'border-top': '1px solid #cfcfcf', 'padding-left': '10px'})
                .text(PROGRESS_COUNT_TEXT + 0 + ": ")
                .hide();
            var progress_bar_container = $('<div/>')
                .addClass('progress-container')
                .css({'border': 'none', 'border-top': '1px solid #cfcfcf'})
            progress_bar = create_progress_bar('progress-bar-warning', 1, 5);
            progress_bar.hide();
            progress_bar.appendTo(progress_bar_container);
            jobs_completed_container.appendTo(input_area);
            progress_bar_container.appendTo(input_area);
        };
    };

    var update_progress_bar = function() {
        var job = cache[0].jobs[0];
        var completed = job.numCompletedTasks;
        var total = job.numTasks;

        var progress_bar = current_cell.element.find('.progress');
        if (progress_bar.length < 1) {
            console.log("No progress bar found");
        };
        update_progress_count(current_cell);

        var progress = completed / total * 100;
        progress_bar.show();
        progress_bar.find('.progress-bar')
            .attr('class', 'progress-bar ' + get_status_class(job.status))
            .attr('aria-valuenow', progress)
            .css('width', progress + '%')
            .text(completed + ' out of ' + total + ' tasks');
    };

    var update_progress_count = function(cell) {
        var progress_count = cell.element.find('.progress_counter');
        if (progress_count.length < 1) {
            console.log("No progress counter found");
        };
        if (spark_is_running) {
            var jobId = cache[0].jobs[0].jobId;
            progress_count.text(PROGRESS_COUNT_TEXT + jobId + ': ');

            var job_name_link = $('<a target="_blank"></a>')
                .attr('href', cache[0].jobs[0].url)
                .text(cache[0].jobs[0].name);
            var cancel_link = $('<a href="#">(cancel job)</a>')
                .on('click', function() { cancel_running_job(jobId) })
                .css({ 'padding-right': '10px', 'float': 'right' });
            progress_count.append(job_name_link);
            progress_count.append(cancel_link);
        };

        progress_count.show();
    };

    var remove_progress_bar = function() {
        if (current_cell != null) {
            var progress_bar_div = current_cell.element.find('.progress-container');
            var progress_count = current_cell.element.find('.progress_counter');
            if (progress_bar_div.length < 1) {
                console.log("No progress bar found");
            };
            progress_count.remove();
            progress_bar_div.remove();

            start_next_progress_bar();
        }
    };

    var start_next_progress_bar = function() {
        cell_queue.shift();
        current_cell = cell_queue[0];
        if (current_cell != null) {
            add_progress_bar(current_cell);
        } else {
            window.clearInterval(current_update_frequency);
            current_update_frequency = window.setInterval(update, UPDATE_FREQUENCY, proxy_url);
        };
    };

    var is_spark_cell = function(cell) {
        // TODO: Find a way to detect if cell is actually running Spark
        return (cell instanceof CodeCell)
    };

    var load_ipython_extension = function () {
        events.on('execute.CodeCell', spark_progress_bar);

        $(document).on('update.progress.bar', update_progress_bar);

        // Kernel becomes idle after a cell finishes executing
        events.on('kernel_idle.Kernel', remove_progress_bar);

        Jupyter.toolbar.add_buttons_group([{
            'label': 'Spark jobs',
            'icon': 'fa-tasks',
            'callback': show_running_jobs,
            'id': 'show_running_jobs'
        }]);
        update(proxy_url);
        current_update_frequency = window.setInterval(update, UPDATE_FREQUENCY, proxy_url);
    };

    return {
        load_ipython_extension: load_ipython_extension
    };
});
