function getDateDelta(dt, days = 1) {
    const dt2 = new Date(dt);
    dt2.setDate(dt2.getDate() + days);

    return dt2.toISOString().substr(0, 10);
}
function scrollToSelected(select_id) {
    var select = document.getElementById(select_id);
    var opts = select.getElementsByTagName('option');
    for (var j = opts.length - 1; j > 0; --j) {
        if (opts.item(j).selected == true) {
            select.scrollTop = j * opts.item(j).offsetHeight;
            return;
        }
    }
}

function fetch_data(params=null) {
    var d = [];
    $.ajax({
        url: '/get_data',
        method: 'POST',
        processData: false,
        dataType: 'json',
        async: false,
        data: params,
        contentType: 'application/json',
        success: function (data) {
            d = data;
        },
        error: function (xhr, status, error) {
            console.log(error);
        }
    });
    return d;
}

function fetch_categories(amcid) {
    var req_data = JSON.stringify({ data: 'categories', amc_id: amcid });
    var d = fetch_data(req_data);
    return d;
}

function fetch_latest_date() {
    var req_data = JSON.stringify({ data: 'latest_date'});
    var d = fetch_data(req_data);
    return d["latest_date"].substr(0,10);
}

function fetch_amcs(amcid) {
    var req_data = JSON.stringify({ data: 'amcs' });
    var d = fetch_data(req_data);
    return d;
}

const app = Vue.createApp({
    data() {
        let latest_date = fetch_latest_date();
        return {
            end_date: latest_date,
            start_date: getDateDelta(latest_date, -1),
            selected_amc: '0',
            selected_cat: ['06'],
            errors: '',
            period_type: 'custom',
        }
    },
    computed: {
        categories() {
            return fetch_categories(this.selected_amc);
        },
        amcs() {
            return fetch_amcs();
        },
    },
    methods: {
        onSubmit(e) {
            e.preventDefault();
            this.fetch_perf();
        },
        amcSelected(e) {
            amcid = e.target.value;
            this.selected_cat = ['0'];
            this.$nextTick(() => { this.fetch_perf() });
        },
        validateForm() {
            this.errors = '';
            if (this.start_date >= this.end_date) {
                this.errors = "Invalid dates";
                return false;
            }

            return true;
        },

        fetch_perf() {
            if (!this.validateForm()) {
                return false;
            }
            var formData = new FormData(document.getElementById('form'));
            const jsonData = {};
            formData.forEach((value, key) => {
                if (key == 'cat_ids') {
                    if (jsonData[key]) {
                        jsonData[key].push(value);
                    } else {
                        jsonData[key] = [value];
                    }
                } else {
                    jsonData[key] = value;
                }
            });
            var json = JSON.stringify(jsonData);

            $("#floatingBarsG").show();
            
            $.ajax({
                url: '/performance',
                method: 'POST',
                data: json,
                processData: false,
                contentType: 'application/json',
                success: function (data) {
                    $('#div-performance').html(data);
                    var table = $('#performance').DataTable({
                        paging: false,
                        order: [[2, 'desc']],
                        // dom: 'frt',
                        buttons: [
                            {
                                extend: 'copy',
                                title: null
                            },
                            {
                                extend: 'excel',
                                title: null
                            },
                            {
                                extend: 'pdf',
                            }
                        ]
                    });
                    table.buttons().container().appendTo( '#div-performance .col-md-6:eq(0)' );
                    $('#performance_info').hide();
                    $("#floatingBarsG").hide();
                },
                error: function (xhr, status, error) {
                    // Data retrieval error
                    console.log(error);
                }
            });
        },
    },
    mounted() {
        // this.start_date = getDateDelta(this.end_date, -1);
        this.fetch_perf();
        scrollToSelected("cat_ids");
    }
});

app.mount('#app');
