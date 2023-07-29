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

function fetch_categories(amcid) {
    var req_data = JSON.stringify({ data: 'categories', amc_id: amcid });
    // console.log(req_data);
    var d = [];
    $.ajax({
        url: '/get_data',
        method: 'POST',
        processData: false,
        dataType: 'json',
        async: false,
        data: req_data,
        contentType: 'application/json',
        success: function (data) {
            d = data;
        },
        error: function (xhr, status, error) {
            // Data retrieval error
            console.log(error);
        }
    });
    return d;
}

function fetch_latest_date(amcid) {
    var req_data = JSON.stringify({ data: 'latest_date'});
    // console.log(req_data);
    var d = [];
    $.ajax({
        url: '/get_data',
        method: 'POST',
        processData: false,
        dataType: 'json',
        async: false,
        data: req_data,
        contentType: 'application/json',
        success: function (data) {
            d = data["latest_date"].substr(0,10);
        },
        error: function (xhr, status, error) {
            // Data retrieval error
            console.log(error);
        }
    });
    return d;
}

function fetch_amcs() {
    var d = [];
    $.ajax({
        url: '/get_data',
        method: 'POST',
        processData: false,
        dataType: 'json',
        async: false,
        data: JSON.stringify({ data: 'amcs' }),
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


const app = Vue.createApp({
    data() {
        return {
            // end_date: getPreviousDay(),
            end_date: fetch_latest_date(),
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
        start_date() {
            return getDateDelta(this.end_date, -1);
        }
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
            // console.log(formData);
            // var json = JSON.stringify(Object.fromEntries(formData));
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
                        // rowGroup: {
                        // dataSrc: 3
                        // },
                        // columnDefs: [
                        //     {
                        //         "targets": [ 1, 2 ],
                        //         "visible": false,
                        //         "searchable": false
                        //     }
                        // ]
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
            // 
        },
    },
    mounted() {
        // this.populate_categories();
        this.fetch_perf();
        scrollToSelected("cat_ids");
    }
});

app.mount('#app');
