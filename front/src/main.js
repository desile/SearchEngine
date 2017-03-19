import Vue from 'vue'
import VueRes from "vue-resource";

Vue.use(VueRes);

var app3 = new Vue({
    el: '#app',
    data: {
        msg: 'The most intelligent and powerful search engine',
        searchType: 'tf_idf',
        searchQuery: '',

        searchResult: [],
        resultSize: 0,
        pageCount: 0,
        showCoef: true
    },
    methods: {
        executeSearchQuery: function(){
            this.$http.get('http://localhost:9090/search', {params: {query: this.searchQuery, type: this.searchType} }).then(response => {
                console.log(response);
                this.searchResult = [];
                this.showCoef = this.searchType == 'tf_idf';
                this.resultSize = response.body.resultSize;
                this.pageCount = Math.ceil(response.body.resultSize / 20);
                response.body.documents.forEach(r => this.searchResult.push(r));
            });
        }
    }
});
