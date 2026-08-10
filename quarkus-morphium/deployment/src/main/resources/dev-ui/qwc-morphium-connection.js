import { LitElement, html, css } from 'lit';
import { JsonRpc } from 'jsonrpc';

export class QwcMorphiumConnection extends LitElement {

    jsonRpc = new JsonRpc(this);

    static properties = {
        _rows: { state: true },
        _loading: { state: true }
    };

    static styles = css`
        :host {
            display: block;
            padding: 1em;
        }
        vaadin-grid {
            width: 100%;
        }
    `;

    constructor() {
        super();
        this._rows = [];
        this._loading = true;
    }

    connectedCallback() {
        super.connectedCallback();
        this.jsonRpc.getConnectionInfo()
            .then(response => {
                this._rows = Array.isArray(response?.result) ? response.result : [];
            })
            .catch(error => {
                console.error('Failed to load connection info', error);
                this._rows = [{
                    Property: 'Status',
                    Value: 'Unable to load connection info: ' + (error?.message ?? 'unknown error')
                }];
            })
            .finally(() => {
                this._loading = false;
            });
    }

    render() {
        if (this._loading) {
            return html`<span>Loading connection info...</span>`;
        }
        return html`
            <vaadin-grid .items=${this._rows} all-rows-visible theme="compact row-stripes no-border">
                <vaadin-grid-column path="Property" header="property"></vaadin-grid-column>
                <vaadin-grid-column path="Value" header="value"></vaadin-grid-column>
            </vaadin-grid>`;
    }
}

customElements.define('qwc-morphium-connection', QwcMorphiumConnection);
